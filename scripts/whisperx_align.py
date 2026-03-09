#!/usr/bin/env python3
import argparse
import json
import os
import sys


def write_error(message):
    print(json.dumps({"error": message}))


def split_tokens(text):
    return [part.strip() for part in str(text or "").split() if str(part or "").strip()]


def token_weight(token):
    clean = "".join(ch for ch in str(token or "") if ch.isalnum())
    return max(1, len(clean) or 1)


def spread_tokens_across_span(tokens, start_sec, end_sec, estimated=True):
    safe_tokens = [token for token in tokens if str(token or "").strip()]
    if not safe_tokens:
        return []
    safe_start = max(0.0, float(start_sec or 0.0))
    safe_end = max(safe_start + 0.08, float(end_sec or safe_start + 0.08))
    span = max(0.08, safe_end - safe_start)
    total = sum(token_weight(token) for token in safe_tokens) or len(safe_tokens)
    out = []
    consumed = 0
    for token in safe_tokens:
        weight = token_weight(token)
        t0 = safe_start + (consumed / total) * span
        consumed += weight
        t1 = safe_start + (consumed / total) * span
        t1 = max(t0 + 0.03, t1)
        out.append(
            {
                "word": str(token).strip(),
                "start_ms": int(round(t0 * 1000)),
                "end_ms": int(round(t1 * 1000)),
                "estimated": bool(estimated),
            }
        )
    return out


def normalize_words(words):
    ordered = sorted(
        [entry for entry in words if entry.get("word")],
        key=lambda item: (int(item.get("start_ms", 0)), int(item.get("end_ms", 0))),
    )
    normalized = []
    prev_end = 0
    for item in ordered:
        text = str(item.get("word") or "").strip()
        if not text:
            continue
        start_ms = int(item.get("start_ms", 0) or 0)
        end_ms = int(item.get("end_ms", 0) or 0)
        start_ms = max(prev_end, start_ms)
        end_ms = max(start_ms + 30, end_ms)
        fixed = {
            "word": text,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "estimated": bool(item.get("estimated", False)),
        }
        score = item.get("score")
        if score is not None:
            try:
                fixed["score"] = float(score)
            except Exception:
                pass
        normalized.append(fixed)
        prev_end = end_ms
    return normalized


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--audio", required=True, help="Path to audio file")
    parser.add_argument("--model", default="small", help="Whisper model name")
    parser.add_argument("--device", default="", help="cpu or cuda (optional)")
    args = parser.parse_args()

    audio_path = args.audio
    if not os.path.exists(audio_path):
        write_error("Audio file not found.")
        sys.exit(1)

    try:
        import torch
    except Exception as exc:
        write_error(f"Missing dependency: {exc}")
        sys.exit(1)

    whisperx = None
    whisper = None
    try:
        import whisperx as whisperx  # type: ignore
    except Exception:
        whisperx = None
    if whisperx is None:
        try:
            import whisper as whisper  # type: ignore
        except Exception as exc:
            write_error(f"Missing dependency: {exc}")
            sys.exit(1)

    device = args.device.strip()
    if not device:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    try:
        segments = []
        words = []
        language = "en"
        engine = "whisperx" if whisperx is not None else "whisper"
        precise_word_timestamps = False
        if whisperx is not None:
            model = whisperx.load_model(args.model, device, compute_type="int8")
            audio = whisperx.load_audio(audio_path)
            result = model.transcribe(audio, batch_size=16)
            language = result.get("language", "en")
            align_model, metadata = whisperx.load_align_model(language_code=language, device=device)
            aligned = whisperx.align(
                result["segments"], align_model, metadata, audio, device, return_char_alignments=False
            )
            for seg in aligned.get("segments", []):
                start = seg.get("start", 0) or 0
                end = seg.get("end", 0) or 0
                text = (seg.get("text") or "").strip()
                if end > start and text:
                    segments.append(
                        {
                            "text": text,
                            "start_ms": int(round(start * 1000)),
                            "end_ms": int(round(end * 1000)),
                        }
                    )
                    timed_words = []
                    for raw_word in seg.get("words", []) or []:
                        word_text = (raw_word.get("word") or raw_word.get("text") or "").strip()
                        if not word_text:
                            continue
                        w_start = raw_word.get("start")
                        w_end = raw_word.get("end")
                        try:
                            w_start = float(w_start) if w_start is not None else None
                            w_end = float(w_end) if w_end is not None else None
                        except Exception:
                            w_start = None
                            w_end = None
                        if w_start is None or w_end is None or w_end <= w_start:
                            continue
                        timed_words.append(
                            {
                                "word": word_text,
                                "start_ms": int(round(max(0.0, w_start) * 1000)),
                                "end_ms": int(round(max(w_start + 0.03, w_end) * 1000)),
                                "estimated": False,
                                "score": raw_word.get("score"),
                            }
                        )
                    if timed_words:
                        words.extend(timed_words)
                        precise_word_timestamps = True
                    else:
                        words.extend(spread_tokens_across_span(split_tokens(text), start, end, estimated=True))
        else:
            model = whisper.load_model(args.model)
            result = model.transcribe(audio_path)
            language = result.get("language", "en")
            for seg in result.get("segments", []):
                start = seg.get("start", 0) or 0
                end = seg.get("end", 0) or 0
                text = (seg.get("text") or "").strip()
                if end > start and text:
                    segments.append(
                        {
                            "text": text,
                            "start_ms": int(round(start * 1000)),
                            "end_ms": int(round(end * 1000)),
                        }
                    )
                    words.extend(spread_tokens_across_span(split_tokens(text), start, end, estimated=True))

        words = normalize_words(words)
        payload = {
            "language": language,
            "segments": segments,
            "words": words,
            "engine": engine,
            "word_timestamps": bool(precise_word_timestamps),
            "timing_quality": "precise" if precise_word_timestamps else "estimated",
        }
        print(json.dumps(payload))
    except Exception as exc:
        write_error(str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
