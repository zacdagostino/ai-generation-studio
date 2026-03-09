#!/usr/bin/env python3
import argparse
import io
import json
import os
import sys

try:
    import cv2
    import numpy as np
except Exception as exc:  # pragma: no cover
    print(json.dumps({"success": False, "error": f"Missing dependency: {exc}"}))
    sys.exit(2)

try:
    from PIL import Image
except Exception:  # pragma: no cover
    Image = None

try:
    from rembg import new_session, remove
except Exception:  # pragma: no cover
    new_session = None
    remove = None


def parse_args():
    parser = argparse.ArgumentParser(description="Extract moving avatar foreground and place on green background.")
    parser.add_argument("--input", required=True, help="Input video path")
    parser.add_argument("--output", required=True, help="Output video path")
    parser.add_argument("--mask-output", default="", help="Optional grayscale mask output video path")
    parser.add_argument("--start", type=float, default=0.0, help="Start time (sec)")
    parser.add_argument("--end", type=float, default=0.0, help="End time (sec), <=0 means full video")
    parser.add_argument("--bg-color", default="0,0,0", help="Background RGB as r,g,b")
    parser.add_argument("--max-frames", type=int, default=0, help="Max frames to process (0 = all)")
    parser.add_argument("--fast-mode", action="store_true", help="Use faster preview extraction (skip heavy matting model)")
    return parser.parse_args()


def fill_holes(mask):
    if mask is None:
        return mask
    flood = mask.copy()
    h, w = mask.shape[:2]
    flood_mask = np.zeros((h + 2, w + 2), np.uint8)
    cv2.floodFill(flood, flood_mask, (0, 0), 255)
    flood_inv = cv2.bitwise_not(flood)
    return cv2.bitwise_or(mask, flood_inv)


def build_default_person_mask(width, height):
    mask = np.zeros((height, width), dtype=np.uint8)
    # Broad central prior for a talking-avatar portrait frame.
    x0 = int(round(width * 0.16))
    y0 = int(round(height * 0.05))
    x1 = int(round(width * 0.84))
    y1 = int(round(height * 0.96))
    cv2.rectangle(mask, (x0, y0), (x1, y1), 255, thickness=-1)
    return mask


def build_anchor_mask(width, height):
    mask = np.zeros((height, width), dtype=np.uint8)
    # Strong prior around torso/head where avatar should be.
    x0 = int(round(width * 0.26))
    y0 = int(round(height * 0.16))
    x1 = int(round(width * 0.74))
    y1 = int(round(height * 0.96))
    cv2.rectangle(mask, (x0, y0), (x1, y1), 255, thickness=-1)
    return mask


def mask_metrics(mask):
    nz = cv2.findNonZero(mask)
    if nz is None:
        return {"area": 0.0, "bw": 0, "bh": 0}
    x, y, w, h = cv2.boundingRect(nz)
    return {"area": float(cv2.countNonZero(mask)), "bw": int(w), "bh": int(h)}


def filter_mask_components(mask, prev_mask, anchor_mask, width, height):
    if mask is None:
        return mask
    num, labels, stats, centroids = cv2.connectedComponentsWithStats(mask, connectivity=8)
    if num <= 1:
        return mask
    total_pixels = float(width * height)
    min_component = max(24.0, total_pixels * 0.00045)
    prev_support = None
    if prev_mask is not None:
        kernel_prev = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (15, 15))
        prev_support = cv2.dilate(prev_mask, kernel_prev, iterations=1)

    best_idx = -1
    best_score = -1e18
    main_bbox = None
    for idx in range(1, num):
        area = float(stats[idx, cv2.CC_STAT_AREA])
        if area < min_component:
            continue
        x = int(stats[idx, cv2.CC_STAT_LEFT])
        y = int(stats[idx, cv2.CC_STAT_TOP])
        w = int(stats[idx, cv2.CC_STAT_WIDTH])
        h = int(stats[idx, cv2.CC_STAT_HEIGHT])
        comp = np.where(labels == idx, 255, 0).astype(np.uint8)
        anchor_overlap = float(cv2.countNonZero(cv2.bitwise_and(comp, anchor_mask)))
        prev_overlap = float(cv2.countNonZero(cv2.bitwise_and(comp, prev_support))) if prev_support is not None else 0.0
        cx = float(centroids[idx][0])
        cy = float(centroids[idx][1])
        dx = (cx - width * 0.5) / max(1.0, width)
        dy = (cy - height * 0.62) / max(1.0, height)
        dist_penalty = (dx * dx + dy * dy) * area * 0.65
        score = area + anchor_overlap * 1.9 + prev_overlap * 2.4 - dist_penalty
        if score > best_score:
            best_score = score
            best_idx = idx
            main_bbox = (x, y, w, h)

    if best_idx < 0:
        return np.zeros_like(mask)

    keep = np.where(labels == best_idx, 255, 0).astype(np.uint8)
    main_area = float(stats[best_idx, cv2.CC_STAT_AREA])
    x, y, w, h = main_bbox
    pad_x = int(round(max(8, width * 0.06)))
    pad_y = int(round(max(8, height * 0.05)))
    ex0 = max(0, x - pad_x)
    ey0 = max(0, y - pad_y)
    ex1 = min(width, x + w + pad_x)
    ey1 = min(height, y + h + pad_y)

    for idx in range(1, num):
        if idx == best_idx:
            continue
        area = float(stats[idx, cv2.CC_STAT_AREA])
        if area < max(min_component, main_area * 0.08):
            continue
        cx = int(round(float(centroids[idx][0])))
        cy = int(round(float(centroids[idx][1])))
        in_expanded = ex0 <= cx < ex1 and ey0 <= cy < ey1
        if not in_expanded:
            continue
        comp = np.where(labels == idx, 255, 0).astype(np.uint8)
        prev_overlap = float(cv2.countNonZero(cv2.bitwise_and(comp, prev_support))) if prev_support is not None else 0.0
        if prev_support is not None and prev_overlap < area * 0.1:
            continue
        keep = cv2.bitwise_or(keep, comp)
    return keep


def extract_mask_with_rembg(frame_bgr, session):
    if remove is None or Image is None:
        return None
    try:
        rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        pil = Image.fromarray(rgb)
        result = remove(pil, session=session)
        if isinstance(result, (bytes, bytearray)):
            result = Image.open(io.BytesIO(result))
        if not isinstance(result, Image.Image):
            arr = np.asarray(result)
        else:
            arr = np.asarray(result.convert("RGBA"))
        if arr.ndim != 3 or arr.shape[2] < 4:
            return None
        alpha = arr[:, :, 3].astype(np.uint8)
        alpha = cv2.GaussianBlur(alpha, (3, 3), 0)
        _, alpha = cv2.threshold(alpha, 16, 255, cv2.THRESH_BINARY)
        return alpha
    except Exception:
        return None


def main():
    args = parse_args()
    def emit_progress(progress, message):
        try:
            pct = int(max(0, min(100, int(progress))))
        except Exception:
            pct = 0
        print(json.dumps({"progress": pct, "message": str(message or "")}), flush=True)

    src = args.input
    dst = args.output
    if not os.path.exists(src):
        print(json.dumps({"success": False, "error": "Input video not found"}))
        return 1

    try:
        bg_rgb = [int(v.strip()) for v in str(args.bg_color).split(",")]
        if len(bg_rgb) != 3:
            raise ValueError("bg-color needs 3 values")
        bg_bgr = np.array([bg_rgb[2], bg_rgb[1], bg_rgb[0]], dtype=np.uint8)
    except Exception:
        bg_bgr = np.array([0, 255, 0], dtype=np.uint8)

    cap = cv2.VideoCapture(src)
    if not cap.isOpened():
        print(json.dumps({"success": False, "error": "Could not open input video"}))
        return 1
    emit_progress(2, "Loading input video")

    fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    duration = (frame_count / fps) if fps > 0 and frame_count > 0 else 0.0

    start_sec = max(0.0, float(args.start or 0.0))
    end_sec = float(args.end or 0.0)
    if end_sec <= 0:
        end_sec = duration if duration > 0 else 1e9
    end_sec = max(start_sec + 0.08, end_sec)

    start_frame = max(0, int(round(start_sec * fps)))
    end_frame = int(round(end_sec * fps)) if fps > 0 else frame_count
    if end_frame <= start_frame:
        end_frame = start_frame + 1

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(dst, fourcc, fps if fps > 0 else 30.0, (width, height))
    if not writer.isOpened():
        print(json.dumps({"success": False, "error": "Could not open output video writer"}))
        cap.release()
        return 1
    mask_writer = None
    if args.mask_output:
        mask_path = args.mask_output
        mask_writer = cv2.VideoWriter(mask_path, fourcc, fps if fps > 0 else 30.0, (width, height), isColor=False)
        if not mask_writer.isOpened():
            print(json.dumps({"success": False, "error": "Could not open mask output video writer"}))
            cap.release()
            writer.release()
            return 1

    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    current = start_frame
    kernel_open = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    kernel_close = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (9, 9))
    gc_mask = np.full((height, width), cv2.GC_BGD, dtype=np.uint8)
    bgd_model = np.zeros((1, 65), np.float64)
    fgd_model = np.zeros((1, 65), np.float64)
    rx = int(round(width * 0.18))
    ry = int(round(height * 0.08))
    rw = max(16, int(round(width * 0.64)))
    rh = max(16, int(round(height * 0.86)))
    rect = (rx, ry, min(rw, width - rx), min(rh, height - ry))
    gc_mask[ry : ry + rect[3], rx : rx + rect[2]] = cv2.GC_PR_FGD
    first = True
    prev_mask = None
    default_mask = build_default_person_mask(width, height)
    anchor_mask = build_anchor_mask(width, height)
    rembg_session = None
    use_rembg = (not args.fast_mode) and remove is not None and new_session is not None and Image is not None
    if use_rembg:
        emit_progress(8, "Loading human matting model")
        try:
            rembg_session = new_session("u2net_human_seg")
        except Exception:
            try:
                rembg_session = new_session()
            except Exception:
                use_rembg = False
        if use_rembg:
            emit_progress(14, "Matting model ready")
        else:
            emit_progress(14, "Matting model unavailable, using fallback extractor")
    elif args.fast_mode:
        emit_progress(14, "Fast preview mode: using fallback extractor")

    total_frames_raw = max(1, end_frame - start_frame)
    max_frames = max(0, int(args.max_frames or 0))
    total_frames = min(total_frames_raw, max_frames) if max_frames > 0 else total_frames_raw
    progress_emit_every = max(1, total_frames // 50)
    processed_frames = 0

    while current < end_frame:
        if max_frames > 0 and processed_frames >= max_frames:
            break
        ok, frame = cap.read()
        if not ok:
            break

        fg_mask = extract_mask_with_rembg(frame, rembg_session) if use_rembg else None
        if fg_mask is None:
            if first:
                cv2.grabCut(frame, gc_mask, rect, bgd_model, fgd_model, 4, cv2.GC_INIT_WITH_RECT)
                first = False
            else:
                # Seed next frame with previous stable silhouette to avoid collapse.
                if prev_mask is not None:
                    gc_mask[:, :] = cv2.GC_BGD
                    gc_mask[default_mask > 0] = cv2.GC_PR_FGD
                    gc_mask[prev_mask > 32] = cv2.GC_FGD
                cv2.grabCut(frame, gc_mask, None, bgd_model, fgd_model, 1, cv2.GC_INIT_WITH_MASK)
            fg_mask = np.where(
                (gc_mask == cv2.GC_FGD) | (gc_mask == cv2.GC_PR_FGD), 255, 0
            ).astype(np.uint8)
        fg_mask = filter_mask_components(fg_mask, prev_mask, anchor_mask, width, height)
        fg_mask = cv2.morphologyEx(fg_mask, cv2.MORPH_OPEN, kernel_open, iterations=1)
        fg_mask = cv2.morphologyEx(fg_mask, cv2.MORPH_CLOSE, kernel_close, iterations=2)
        fg_mask = fill_holes(fg_mask)
        if prev_mask is not None:
            # Temporal stabilization: preserve previous silhouette support to avoid sudden collapses.
            prev_support = cv2.erode(prev_mask, kernel_open, iterations=1)
            fg_mask = cv2.bitwise_or(fg_mask, prev_support)
        # Guard against tiny accidental fragments by reusing last good mask.
        metrics = mask_metrics(fg_mask)
        area = float(metrics["area"])
        min_area = float(width * height) * 0.015
        max_area = float(width * height) * 0.95
        min_bw = int(round(width * 0.22))
        min_bh = int(round(height * 0.38))
        is_too_small = area < min_area or metrics["bw"] < min_bw or metrics["bh"] < min_bh
        if is_too_small:
            if prev_mask is not None:
                fg_mask = prev_mask.copy()
            else:
                fg_mask = default_mask.copy()
        elif area > max_area and prev_mask is not None:
            fg_mask = prev_mask.copy()
        else:
            prev_mask = fg_mask.copy()
        fg_mask = cv2.GaussianBlur(fg_mask, (5, 5), 0)

        alpha = (fg_mask.astype(np.float32) / 255.0)[..., None]
        bg = np.full_like(frame, bg_bgr)
        comp = (frame.astype(np.float32) * alpha + bg.astype(np.float32) * (1.0 - alpha)).astype(np.uint8)
        writer.write(comp)
        if mask_writer is not None:
            mask_writer.write(fg_mask)
        current += 1
        processed_frames += 1
        processed = processed_frames
        if processed == 1 or processed % progress_emit_every == 0 or processed >= total_frames:
            pct = max(0, min(100, int(round((processed / total_frames) * 100))))
            emit_progress(pct, f"Processing frame {processed}/{total_frames}")

    cap.release()
    writer.release()
    if mask_writer is not None:
        mask_writer.release()

    print(json.dumps({"success": True, "output": dst, "maskOutput": args.mask_output or ""}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
