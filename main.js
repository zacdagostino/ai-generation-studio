const { app, BrowserWindow, ipcMain, dialog, Menu, shell, clipboard, session } = require('electron');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { Blob, File } = require('buffer');
const { spawn, spawnSync } = require('child_process');

// Electron/Node combos may not expose File globally; undici expects it on load.
if (typeof globalThis.Blob === 'undefined') globalThis.Blob = Blob;
if (typeof globalThis.File === 'undefined' && typeof File !== 'undefined') globalThis.File = File;

const { fetch, Agent, FormData } = require('undici');
const ytdl = require('@distube/ytdl-core');

const LEGACY_USER_DATA_DIR = path.join(app.getPath('appData'), 'amelia_app');

const FILE_STREAM_UPLOAD_URL = 'https://kieai.redpandaai.co/api/file-stream-upload';
const DEFAULT_UPLOAD_PATH = 'images/user-uploads';
const RUNPOD_GRAPHQL_URL = 'https://api.runpod.io/graphql';
const KIE_LOGS_AUTH_PARTITION = 'persist:kie-logs-auth';
const KIE_LOGS_URL = 'https://kie.ai/logs';
const KIE_LOGS_CONNECT_PROGRESS_CHANNEL = 'kie-logs-connect-progress';
const KIE_LOGS_SCRAPE_PROGRESS_CHANNEL = 'kie-logs-scrape-progress';
const KIE_LOGS_BRIDGE_VISIBLE = false;
// Store persisted blobs outside the project root to avoid dev reloads on file writes.
const CACHE_DIR = path.join(app.getPath('userData'), 'cache_uploads');
const SAVED_PROJECTS_FILE = path.join(app.getPath('userData'), 'saved_projects_v1.json');
const DEBUG_TRACE_FILE = path.join(app.getPath('userData'), 'debug_trace.log');
const YOUTUBE_CACHE_DIR = path.join(app.getPath('userData'), 'youtube_cache');
const TIKTOK_MEDIA_CACHE_DIR = path.join(app.getPath('userData'), 'tiktok_media_cache');
const TIKTOK_SFX_PREVIEW_CACHE_DIR = path.join(app.getPath('userData'), 'tiktok_sfx_preview_cache');
const TIKTOK_AUDIO_SEGMENTS_CACHE_DIR = path.join(app.getPath('userData'), 'tiktok_audio_segments_cache');

if (process.platform === 'linux') {
  // Avoid GPU/VAAPI startup crashes on older Linux stacks.
  app.disableHardwareAcceleration();
  app.commandLine.appendSwitch('disable-gpu');
  app.commandLine.appendSwitch('disable-gpu-compositing');
  app.commandLine.appendSwitch('disable-vulkan');
  app.commandLine.appendSwitch('use-angle', 'swiftshader');
  app.commandLine.appendSwitch('use-gl', 'swiftshader');
  app.commandLine.appendSwitch('disable-features', 'VaapiVideoDecoder,VaapiVideoEncoder');
}

let mainWindow = null;
let allowClose = false;
let hotReloadWatchers = [];
let hotReloadTimer = null;
let kieLogsBridgeWindow = null;
let kieLogsLastBearerToken = '';
let kieLogsAuthCaptureBound = false;
let kieLogsBridgeEnsureInFlight = null;
const activeTikTokExports = new Map();
const CAPTION_RENDER_PIPELINE_VERSION = 'caption-render-2026-02-09m';

const appendDebugTrace = (source = 'main', message = '') => {
  const text = String(message || '').trim();
  if (!text) return;
  try {
    const line = `[${new Date().toISOString()}] [${source}] ${text}\n`;
    try {
      const stat = fs.statSync(DEBUG_TRACE_FILE);
      if (stat.size > 2 * 1024 * 1024) {
        fs.writeFileSync(DEBUG_TRACE_FILE, line, 'utf8');
        return;
      }
    } catch (_err) {}
    fs.appendFileSync(DEBUG_TRACE_FILE, line, 'utf8');
  } catch (_err) {}
};

const copyTextToClipboardSafe = (text = '') => {
  const value = String(text || '').trim();
  if (!value) return false;
  try {
    clipboard.writeText(value);
    return true;
  } catch (_err) {
    return false;
  }
};

const emitKieLogsConnectProgress = (sender, payload = {}) => {
  try {
    if (!sender || typeof sender.send !== 'function' || sender.isDestroyed?.()) return;
    const now = Date.now();
    sender.send(KIE_LOGS_CONNECT_PROGRESS_CHANNEL, {
      at: now,
      step: String(payload.step || '').trim(),
      status: String(payload.status || 'active').trim().toLowerCase(),
      detail: String(payload.detail || '').trim(),
      elapsedMs: Math.max(0, Number(payload.elapsedMs || 0)),
      meta: payload?.meta && typeof payload.meta === 'object' ? payload.meta : undefined
    });
  } catch (_err) {}
};

const emitKieLogsScrapeProgress = (sender, payload = {}) => {
  try {
    if (!sender || typeof sender.send !== 'function' || sender.isDestroyed?.()) return;
    const now = Date.now();
    sender.send(KIE_LOGS_SCRAPE_PROGRESS_CHANNEL, {
      at: now,
      stage: String(payload.stage || '').trim() || 'info',
      level: String(payload.level || 'info').trim().toLowerCase(),
      detail: String(payload.detail || '').trim(),
      meta: payload?.meta && typeof payload.meta === 'object' ? payload.meta : undefined
    });
  } catch (_err) {}
};

const probeKieLogsUiReady = async (webContents) => {
  if (!webContents || webContents.isDestroyed()) {
    return { ready: false, reason: 'webcontents-unavailable' };
  }
  const script = `(() => {
    const q = (sel) => {
      try { return document.querySelector(sel); } catch (_err) { return null; }
    };
    const qa = (sel) => {
      try { return Array.from(document.querySelectorAll(sel)); } catch (_err) { return []; }
    };
    const bodyText = String(document.body?.innerText || '').slice(0, 16000).toLowerCase();
    const paginationNav = q('nav[data-slot="base"][data-total], [data-slot="base"][data-total][data-active-page], nav[role="navigation"][aria-label*="pagination" i]');
    const goToInput = q('input[data-slot="input"][type="number"], input[aria-label*="go" i][type="number"], input[aria-label*="page" i][type="number"]');
    const rowCandidates = qa('[data-slot="tbody"] tr, tbody tr, [role="row"]');
    const visibleRows = rowCandidates.filter((row) => {
      const rect = row?.getBoundingClientRect?.();
      return !!rect && rect.width > 24 && rect.height > 12;
    });
    const taskHints = /task\\s*id|recent\\s*history|credits\\s*consumed|model\\s*&\\s*details/.test(bodyText);
    const resultButtons = qa('button,[role="button"],a').filter((el) => {
      const text = String(el?.innerText || el?.textContent || el?.getAttribute?.('aria-label') || '').toLowerCase();
      return /\\bresult\\b/.test(text);
    });
    const hasHexTaskToken = /\\b[0-9a-f]{24,64}\\b/i.test(String(document.body?.innerText || '').slice(0, 20000));
    const signals = {
      pagination: !!paginationNav,
      goToInput: !!goToInput,
      visibleRows: Number(visibleRows.length || 0),
      resultButtons: Number(resultButtons.length || 0),
      taskHints: !!taskHints,
      hexTaskToken: !!hasHexTaskToken
    };
    const ready =
      signals.pagination ||
      signals.goToInput ||
      signals.visibleRows >= 2 ||
      signals.resultButtons >= 1 ||
      signals.taskHints ||
      signals.hexTaskToken;
    return {
      ready,
      reason: ready ? 'ui-signals-detected' : 'waiting-ui-signals',
      signals
    };
  })();`;
  try {
    const res = await webContents.executeJavaScript(script, true);
    if (res && typeof res === 'object') return res;
  } catch (err) {
    return { ready: false, reason: `probe-error:${String(err?.message || 'unknown')}` };
  }
  return { ready: false, reason: 'probe-empty' };
};

const sanitizeImageSequencePrefix = (value = '') => {
  const cleaned = String(value || '')
    .trim()
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '')
    .replace(/\s+/g, '-');
  return cleaned || 'image';
};

const sanitizeImageFilename = (value = '') => {
  const cleaned = String(value || '')
    .trim()
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '')
    .replace(/\s+/g, '-');
  const withoutExt = cleaned.replace(/\.[a-zA-Z0-9]{1,8}$/g, '');
  const base = withoutExt || 'image';
  return `${base}.webp`;
};

const escapeRegExp = (value = '') => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const sanitizeFfmpegArgs = (args = []) => {
  const raw = Array.isArray(args) ? args : [];
  const dropWithValue = new Set(['-preset', '-tune']);
  const sanitized = [];
  for (let i = 0; i < raw.length; i += 1) {
    const token = String(raw[i] ?? '');
    if (dropWithValue.has(token)) {
      i += 1;
      continue;
    }
    sanitized.push(raw[i]);
  }
  return sanitized;
};

const sanitizeHttpUrl = (value = '') => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    if (!/^https?:$/i.test(parsed.protocol)) return '';
    return parsed.toString();
  } catch (_err) {
    return '';
  }
};

const sanitizeRunpodApiKey = (value = '') => String(value || '').trim();

const normalizeRunpodPort = (port = {}) => {
  if (!port || typeof port !== 'object') return null;
  const ip = String(port.ip || '').trim();
  const privatePort = Number(port.privatePort || 0);
  const publicPort = Number(port.publicPort || 0);
  const type = String(port.type || '').trim();
  if (!ip && !privatePort && !publicPort) return null;
  return {
    ip,
    isIpPublic: !!port.isIpPublic,
    privatePort: Number.isFinite(privatePort) ? privatePort : 0,
    publicPort: Number.isFinite(publicPort) ? publicPort : 0,
    type
  };
};

const normalizeRunpodPod = (pod = {}) => {
  if (!pod || typeof pod !== 'object') return null;
  const id = String(pod.id || '').trim();
  if (!id) return null;
  const runtime = pod.runtime && typeof pod.runtime === 'object' ? pod.runtime : {};
  const ports = Array.isArray(runtime.ports) ? runtime.ports.map(normalizeRunpodPort).filter(Boolean) : [];
  return {
    id,
    name: String(pod.name || '').trim() || id,
    desiredStatus: String(pod.desiredStatus || pod.status || '').trim(),
    imageName: String(pod.imageName || '').trim(),
    machine: pod.machine && typeof pod.machine === 'object' ? pod.machine : {},
    runtime: {
      uptimeInSeconds: Number(runtime.uptimeInSeconds || 0) || 0,
      ports
    }
  };
};

const extractRunpodPods = (data = {}) => {
  const direct = Array.isArray(data?.myself?.pods) ? data.myself.pods : null;
  if (direct) return direct.map(normalizeRunpodPod).filter(Boolean);
  const alt = Array.isArray(data?.pods) ? data.pods : null;
  if (alt) return alt.map(normalizeRunpodPod).filter(Boolean);
  return [];
};

const runpodGraphqlRequest = async (apiKey = '', query = '', variables = {}) => {
  const key = sanitizeRunpodApiKey(apiKey);
  if (!key) throw new Error('RunPod API key is required.');
  const queryText = String(query || '').trim();
  if (!queryText) throw new Error('RunPod query is empty.');
  const body = JSON.stringify({ query: queryText, variables: variables && typeof variables === 'object' ? variables : {} });
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20_000);
  try {
    const res = await fetch(RUNPOD_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        Authorization: `Bearer ${key}`,
        'Api-Key': key
      },
      body,
      signal: controller.signal
    });
    const text = await res.text();
    let payload = {};
    try {
      payload = text ? JSON.parse(text) : {};
    } catch (_err) {
      throw new Error(`RunPod returned invalid JSON (HTTP ${res.status}).`);
    }
    if (!res.ok) {
      const errMsg =
        String(payload?.errors?.[0]?.message || payload?.message || `RunPod API returned HTTP ${res.status}.`).trim() ||
        `RunPod API returned HTTP ${res.status}.`;
      throw new Error(errMsg);
    }
    if (Array.isArray(payload?.errors) && payload.errors.length) {
      throw new Error(String(payload.errors[0]?.message || 'RunPod GraphQL error.').trim() || 'RunPod GraphQL error.');
    }
    return payload?.data && typeof payload.data === 'object' ? payload.data : {};
  } catch (err) {
    if (err?.name === 'AbortError') throw new Error('RunPod request timed out.');
    throw err;
  } finally {
    clearTimeout(timeout);
  }
};

const RUNPOD_LIST_PODS_QUERY = `
query ContentStudioPods {
  myself {
    pods {
      id
      name
      desiredStatus
      imageName
      runtime {
        uptimeInSeconds
        ports {
          ip
          isIpPublic
          privatePort
          publicPort
          type
        }
      }
      machine {
        gpuDisplayName
      }
    }
  }
}
`;

const RUNPOD_LIST_PODS_QUERY_FALLBACK = `
query ContentStudioPodsFallback {
  myself {
    pods {
      id
      name
      desiredStatus
    }
  }
}
`;

const RUNPOD_GET_POD_QUERY = `
query ContentStudioPodById($podId: String!) {
  pod(input: { podId: $podId }) {
    id
    name
    desiredStatus
    imageName
    runtime {
      uptimeInSeconds
      ports {
        ip
        isIpPublic
        privatePort
        publicPort
        type
      }
    }
    machine {
      gpuDisplayName
    }
  }
}
`;

const runAvatarForegroundExtraction = async ({
  inputPath = '',
  outputPath = '',
  maskOutputPath = '',
  startSec = 0,
  endSec = 0,
  maxFrames = 0,
  fastMode = false,
  onProgress = null
} = {}) => {
  const inPath = String(inputPath || '').trim();
  const outPath = String(outputPath || '').trim();
  if (!inPath) throw new Error('Foreground extraction input path is empty.');
  if (!outPath) throw new Error('Foreground extraction output path is empty.');
  const maskPath = String(maskOutputPath || '').trim();
  const scriptPath = path.join(__dirname, 'scripts', 'extract_avatar_foreground.py');
  if (!fs.existsSync(scriptPath)) {
    throw new Error(`Foreground extractor script not found: ${scriptPath}`);
  }
  const start = Math.max(0, Number(startSec || 0));
  const end = Math.max(start + 0.08, Number(endSec || start + 1));
  const pyArgs = [
    scriptPath,
    '--input',
    inPath,
    '--output',
    outPath,
    '--start',
    String(start),
    '--end',
    String(end),
    '--bg-color',
    '0,0,0'
  ];
  if (maskPath) {
    pyArgs.push('--mask-output', maskPath);
  }
  if (Number(maxFrames) > 0) {
    pyArgs.push('--max-frames', String(Math.max(1, Math.round(Number(maxFrames)))));
  }
  if (fastMode) {
    pyArgs.push('--fast-mode');
  }
  const reportProgress = (percent, message = '') => {
    if (typeof onProgress !== 'function') return;
    const pct = Number(percent);
    if (!Number.isFinite(pct) && !String(message || '').trim()) return;
    onProgress(Number.isFinite(pct) ? pct : NaN, String(message || '').trim());
  };
  const runPythonCommand = (pythonBin, args = []) =>
    new Promise((resolve, reject) => {
      const proc = spawn(pythonBin, args, { stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '';
      let err = '';
      proc.stdout.on('data', (d) => {
        out += d.toString();
      });
      proc.stderr.on('data', (d) => {
        err += d.toString();
      });
      proc.on('error', (spawnErr) => reject(spawnErr));
      proc.on('close', (code) => {
        if (code !== 0) {
          reject(new Error(String(err || out || `python exited with ${code}`).trim()));
          return;
        }
        resolve({ out, err });
      });
    });
  const ensureModuleAvailable = async (pythonBin, moduleNameRaw = '') => {
    const moduleName = String(moduleNameRaw || '').trim();
    if (!moduleName) return false;
    const installPlanByModule = {
      cv2: ['opencv-python-headless'],
      rembg: ['rembg', 'onnxruntime', 'pillow'],
      PIL: ['pillow'],
      onnxruntime: ['onnxruntime']
    };
    const packages = installPlanByModule[moduleName] || [moduleName];
    try {
      reportProgress(5, `Checking Python module: ${moduleName}`);
      await runPythonCommand(pythonBin, ['-c', `import ${moduleName}`]);
      reportProgress(8, `Module ready: ${moduleName}`);
      return true;
    } catch (_importErr) {
      reportProgress(10, `Installing missing module: ${moduleName}`);
      const installAttempts = [
        ['-m', 'pip', 'install', '--user', '--disable-pip-version-check', ...packages],
        ['-m', 'pip', 'install', '--disable-pip-version-check', ...packages]
      ];
      for (const installArgs of installAttempts) {
        try {
          await runPythonCommand(pythonBin, installArgs);
          await runPythonCommand(pythonBin, ['-c', `import ${moduleName}`]);
          reportProgress(14, `Installed module: ${moduleName}`);
          return true;
        } catch (_installErr) {}
      }
      return false;
    }
  };
  const tryRun = (pythonBin) =>
    new Promise((resolve, reject) => {
      const proc = spawn(pythonBin, pyArgs, { stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '';
      let err = '';
      let stdoutBuffer = '';
      proc.stdout.on('data', (d) => {
        const text = d.toString();
        out += text;
        stdoutBuffer += text;
        const lines = stdoutBuffer.split(/\r?\n/);
        stdoutBuffer = lines.pop() || '';
        lines.forEach((lineRaw) => {
          const line = String(lineRaw || '').trim();
          if (!line) return;
          try {
            const parsed = JSON.parse(line);
            const msg = String(parsed?.message || '').trim();
            const pctRaw = Number(parsed?.progress);
            if (typeof onProgress === 'function' && (Number.isFinite(pctRaw) || msg)) {
              onProgress(Number.isFinite(pctRaw) ? Math.max(0, Math.min(100, pctRaw)) : NaN, msg);
            }
          } catch (_err) {}
        });
      });
      proc.stderr.on('data', (d) => {
        err += d.toString();
      });
      proc.on('error', (spawnErr) => reject(spawnErr));
      proc.on('close', (code) => {
        if (code !== 0) {
          const msg = String(err || out || `extractor exited with ${code}`).trim();
          const e = new Error(msg);
          const moduleMatch = msg.match(/No module named ['"]?([A-Za-z0-9_.-]+)['"]?/i);
          if (moduleMatch) {
            e.code = 'MISSING_PY_MODULE';
            e.moduleName = String(moduleMatch[1] || '').trim();
          }
          reject(e);
          return;
        }
        const lines = String(out || '')
          .split(/\r?\n/)
          .map((v) => v.trim())
          .filter(Boolean);
        let parsed = null;
        for (let i = lines.length - 1; i >= 0; i -= 1) {
          try {
            parsed = JSON.parse(lines[i]);
            break;
          } catch (_err) {}
        }
        if (parsed?.success === false) {
          reject(new Error(String(parsed.error || 'Foreground extraction failed.')));
          return;
        }
        resolve(parsed || {});
      });
    });
  let firstErr = null;
  let parsedResult = {};
  const pythonBins = ['python3', 'python'];
  const requiredModelModules = ['cv2', 'rembg', 'PIL'];
  reportProgress(2, 'Starting Python foreground extraction');
  for (const bin of pythonBins) {
    reportProgress(3, `Checking Python runtime: ${bin}`);
    let modulesReady = true;
    for (let modIdx = 0; modIdx < requiredModelModules.length; modIdx += 1) {
      const mod = requiredModelModules[modIdx];
      const checkPct = 4 + Math.round((modIdx / Math.max(1, requiredModelModules.length)) * 18);
      reportProgress(checkPct, `Verifying dependency: ${mod}`);
      // Enforce model-based pipeline dependencies before running extraction.
      // If install fails for this interpreter, we try the next python binary.
      // This guarantees option-1 behavior instead of silent heuristic fallback.
      // eslint-disable-next-line no-await-in-loop
      const ready = await ensureModuleAvailable(bin, mod);
      if (!ready) {
        modulesReady = false;
        if (!firstErr) firstErr = new Error(`Missing dependency '${mod}' for avatar matting model.`);
        break;
      }
    }
    if (!modulesReady) continue;
    reportProgress(24, 'Launching matting model');
    try {
      parsedResult = await tryRun(bin);
      reportProgress(100, 'Foreground extraction complete');
      firstErr = null;
      break;
    } catch (err) {
      if (!firstErr) firstErr = err;
      if (err?.code === 'MISSING_PY_MODULE' && err?.moduleName) {
        const ready = await ensureModuleAvailable(bin, err.moduleName);
        if (ready) {
          parsedResult = await tryRun(bin);
          firstErr = null;
          break;
        }
      }
    }
  }
  if (firstErr) {
    const missing = String(firstErr?.message || '').match(/No module named ['"]?([A-Za-z0-9_.-]+)['"]?/i);
    if (missing) {
      const moduleName = String(missing[1] || '').trim();
      const installHint =
        moduleName === 'cv2'
          ? 'opencv-python-headless'
          : moduleName === 'rembg'
          ? 'rembg onnxruntime pillow'
          : moduleName === 'PIL'
          ? 'pillow'
          : moduleName || 'required-package';
      throw new Error(
        `${firstErr.message} (auto-install failed). Install with: python3 -m pip install ${installHint}`
      );
    }
    throw firstErr;
  }
  if (!fs.existsSync(outPath)) {
    throw new Error('Foreground extraction did not produce output video.');
  }
  const outMask = String(parsedResult?.maskOutput || maskPath || '').trim();
  return {
    colorPath: outPath,
    maskPath: outMask && fs.existsSync(outMask) ? outMask : ''
  };
};

const downloadRemoteMediaToFile = async (url = '', outputPath = '') => {
  const rawUrl = String(url || '').trim();
  const outPath = String(outputPath || '').trim();
  if (!rawUrl) throw new Error('Remote media URL is empty.');
  if (!outPath) throw new Error('Remote media output path is empty.');
  const res = await fetch(rawUrl, { redirect: 'follow' });
  if (!res.ok) {
    throw new Error(`Failed to download remote media (${res.status})`);
  }
  const arrayBuffer = await res.arrayBuffer();
  const buf = Buffer.from(arrayBuffer);
  fs.writeFileSync(outPath, buf);
  if (!fs.existsSync(outPath) || fs.statSync(outPath).size <= 0) {
    throw new Error('Remote media download produced an empty file.');
  }
  return outPath;
};

const hasLevelDbData = (dirPath) => {
  const levelDbDir = path.join(dirPath, 'Local Storage', 'leveldb');
  if (!fs.existsSync(levelDbDir)) return false;
  try {
    const entries = fs.readdirSync(levelDbDir);
    return entries.some((name) => name.endsWith('.ldb'));
  } catch (_err) {
    return false;
  }
};

const hasStorageKeyInLevelDb = (dirPath, storageKey = '') => {
  const targetKey = String(storageKey || '').trim();
  if (!targetKey) return false;
  const levelDbDir = path.join(dirPath, 'Local Storage', 'leveldb');
  if (!fs.existsSync(levelDbDir)) return false;
  let files = [];
  try {
    files = fs
      .readdirSync(levelDbDir)
      .filter((name) => /\.(ldb|log)$/i.test(name))
      .map((name) => path.join(levelDbDir, name));
  } catch (_err) {
    return false;
  }
  if (!files.length) return false;
  try {
    const result = spawnSync('strings', ['-a', ...files], {
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024
    });
    const output = String(result?.stdout || '');
    if (!output) return false;
    if (output.includes(`\n${targetKey}\n`)) return true;
    return output.includes(targetKey);
  } catch (_err) {
    return false;
  }
};

const migrateLegacyUserData = () => {
  const currentUserData = app.getPath('userData');
  if (currentUserData === LEGACY_USER_DATA_DIR) return;
  if (!fs.existsSync(LEGACY_USER_DATA_DIR)) return;
  const legacyHasData = hasLevelDbData(LEGACY_USER_DATA_DIR);
  const currentHasData = hasLevelDbData(currentUserData);
  if (!legacyHasData) return;
  const legacyHasAvatarOrObjectData =
    hasStorageKeyInLevelDb(LEGACY_USER_DATA_DIR, 'nb_avatars_v1') || hasStorageKeyInLevelDb(LEGACY_USER_DATA_DIR, 'nb_objects_v1');
  const currentHasAvatarOrObjectData =
    hasStorageKeyInLevelDb(currentUserData, 'nb_avatars_v1') || hasStorageKeyInLevelDb(currentUserData, 'nb_objects_v1');
  const shouldFullMigrate = !currentHasData;
  const shouldOverlayLocalStorage = currentHasData && legacyHasAvatarOrObjectData && !currentHasAvatarOrObjectData;
  if (!shouldFullMigrate && !shouldOverlayLocalStorage) return;
  fs.mkdirSync(currentUserData, { recursive: true });
  const items = shouldFullMigrate ? ['Local Storage', 'Session Storage', 'cache_uploads'] : ['Local Storage'];
  items.forEach((name) => {
    const src = path.join(LEGACY_USER_DATA_DIR, name);
    const dest = path.join(currentUserData, name);
    if (!fs.existsSync(src)) return;
    if (fs.existsSync(dest)) {
      const backup = `${dest}.backup-${Date.now()}`;
      try {
        fs.cpSync(dest, backup, { recursive: true });
      } catch (_err) {}
      fs.rmSync(dest, { recursive: true, force: true });
    }
    fs.cpSync(src, dest, { recursive: true });
  });
  if (shouldOverlayLocalStorage) {
    console.log('[main] Recovered legacy Local Storage (avatars/objects) into current profile.');
  } else {
    console.log('[main] Migrated legacy user data to new app name folder.');
  }
};

const recoverLocalStorageArraySnapshotFromDir = (storageKey = '', userDataDir = '') => {
  const targetKey = String(storageKey || '').trim();
  if (!targetKey) return [];
  const baseDir = String(userDataDir || '').trim() || app.getPath('userData');
  const levelDbDir = path.join(baseDir, 'Local Storage', 'leveldb');
  if (!fs.existsSync(levelDbDir)) return [];
  let files = [];
  try {
    files = fs
      .readdirSync(levelDbDir)
      .filter((name) => /\.(ldb|log)$/i.test(name))
      .map((name) => path.join(levelDbDir, name));
  } catch (_err) {
    return [];
  }
  if (!files.length) return [];
  try {
    const result = spawnSync('strings', ['-a', ...files], {
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024
    });
    const output = String(result?.stdout || '');
    if (!output) return [];
    const lines = output.split(/\r?\n/);
    let best = [];
    let bestLength = 0;
    for (let index = 0; index < lines.length - 1; index += 1) {
      if (String(lines[index] || '').trim() !== targetKey) continue;
      const candidate = String(lines[index + 1] || '').trim();
      if (!candidate.startsWith('[')) continue;
      try {
        const parsed = JSON.parse(candidate);
        if (Array.isArray(parsed) && candidate.length > bestLength) {
          best = parsed;
          bestLength = candidate.length;
        }
      } catch (_err) {}
    }
    return best;
  } catch (_err) {
    return [];
  }
};

const recoverLocalStorageArraySnapshot = (storageKey = '') => {
  const targetKey = String(storageKey || '').trim();
  if (!targetKey) return [];
  const currentItems = recoverLocalStorageArraySnapshotFromDir(targetKey, app.getPath('userData'));
  if (Array.isArray(currentItems) && currentItems.length) return currentItems;
  if (app.getPath('userData') === LEGACY_USER_DATA_DIR) return currentItems;
  const legacyItems = recoverLocalStorageArraySnapshotFromDir(targetKey, LEGACY_USER_DATA_DIR);
  return Array.isArray(legacyItems) ? legacyItems : [];
};

const loadSavedProjectsFromDisk = () => {
  try {
    if (!fs.existsSync(SAVED_PROJECTS_FILE)) return [];
    const raw = fs.readFileSync(SAVED_PROJECTS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_err) {
    return [];
  }
};

const persistSavedProjectsToDisk = (projects = []) => {
  const list = Array.isArray(projects) ? projects : [];
  const dir = path.dirname(SAVED_PROJECTS_FILE);
  fs.mkdirSync(dir, { recursive: true });
  const tempFile = `${SAVED_PROJECTS_FILE}.tmp`;
  fs.writeFileSync(tempFile, JSON.stringify(list, null, 2), 'utf8');
  fs.renameSync(tempFile, SAVED_PROJECTS_FILE);
  return true;
};

const createWindow = () => {
  allowClose = false;
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 900,
    webPreferences: {
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    }
  });

  mainWindow.loadFile(path.join(__dirname, 'index.html'));
  if (mainWindow.webContents && typeof mainWindow.webContents.on === 'function') {
    mainWindow.webContents.on('render-process-gone', (_event, details) => {
      console.error('[main] render-process-gone:', details);
    });
    mainWindow.webContents.on('did-fail-load', (_event, code, description, validatedURL, isMainFrame) => {
      if (isMainFrame) {
        console.error('[main] did-fail-load:', { code, description, validatedURL });
      }
    });
  }

  mainWindow.on('close', (event) => {
    if (allowClose) return;
    event.preventDefault();
    if (!mainWindow?.webContents || mainWindow.webContents.isDestroyed()) {
      allowClose = true;
      mainWindow?.close();
      return;
    }
    mainWindow.webContents
      .executeJavaScript('window.__confirmAppClose?.()')
      .then((allow) => {
        if (allow === false) return;
        allowClose = true;
        mainWindow.close();
      })
      .catch(() => {
        allowClose = true;
        mainWindow.close();
      });
  });

  mainWindow.on('closed', () => {
    if (kieLogsBridgeWindow && !kieLogsBridgeWindow.isDestroyed()) {
      try {
        kieLogsBridgeWindow.destroy();
      } catch (_err) {}
    }
    kieLogsBridgeWindow = null;
    mainWindow = null;
  });
};

const stopHotReloadWatchers = () => {
  hotReloadWatchers.forEach((watcher) => {
    try {
      watcher.close();
    } catch (_err) {}
  });
  hotReloadWatchers = [];
  if (hotReloadTimer) {
    clearTimeout(hotReloadTimer);
    hotReloadTimer = null;
  }
};

const scheduleRendererReload = (reason = '') => {
  if (hotReloadTimer) clearTimeout(hotReloadTimer);
  hotReloadTimer = setTimeout(() => {
    hotReloadTimer = null;
    if (!mainWindow?.webContents || mainWindow.webContents.isDestroyed()) return;
    if (mainWindow.webContents.isLoading()) return;
    console.log(`[main] Hot reload renderer (${reason || 'file change'})`);
    mainWindow.webContents.reloadIgnoringCache();
  }, 180);
};

const startRendererHotReload = () => {
  if (app.isPackaged) return;
  stopHotReloadWatchers();
  const watchFiles = ['index.html', 'renderer.js', 'preload.js']
    .map((name) => path.join(__dirname, name))
    .filter((filePath) => fs.existsSync(filePath));
  watchFiles.forEach((filePath) => {
    try {
      const watcher = fs.watch(filePath, { persistent: true }, () => {
        scheduleRendererReload(path.basename(filePath));
      });
      hotReloadWatchers.push(watcher);
    } catch (err) {
      console.warn(`[main] Could not watch ${filePath}: ${err.message}`);
    }
  });
};

const sendToRenderer = (channel, payload) => {
  if (!mainWindow?.webContents || mainWindow.webContents.isDestroyed()) return;
  mainWindow.webContents.send(channel, payload);
};

const createAppMenu = () => {
  const isMac = process.platform === 'darwin';
  const template = [
    ...(isMac
      ? [
          {
            label: app.name,
            submenu: [{ role: 'about' }, { type: 'separator' }, { role: 'quit' }]
          }
        ]
      : []),
    {
      label: 'File',
      submenu: [
        {
          label: 'Save',
          accelerator: 'CmdOrCtrl+S',
          click: () => sendToRenderer('menu-save')
        },
        { type: 'separator' },
        isMac ? { role: 'close' } : { role: 'quit' }
      ]
    },
    { role: 'editMenu' },
    { role: 'viewMenu' },
    { role: 'windowMenu' }
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
};

app.whenReady().then(() => {
  migrateLegacyUserData();
  createWindow();
  startRendererHotReload();
  createAppMenu();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  stopHotReloadWatchers();
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

ipcMain.on('app-close-response', (_event, payload = {}) => {
  const allow = !!payload.allow;
  if (!allow || !mainWindow) return;
  allowClose = true;
  mainWindow.close();
});

const waitForBrowserWindowLoad = (win, { timeoutMs = 45_000, onProgress = null } = {}) =>
  new Promise((resolve, reject) => {
    if (!win || win.isDestroyed()) {
      reject(new Error('Kie logs window is unavailable.'));
      return;
    }
    const contents = win.webContents;
    if (!contents || contents.isDestroyed()) {
      reject(new Error('Kie logs web contents is unavailable.'));
      return;
    }

    const isLoading = () => {
      if (!contents || contents.isDestroyed()) return false;
      if (typeof contents.isLoadingMainFrame === 'function') {
        return contents.isLoadingMainFrame();
      }
      return contents.isLoading();
    };

    const isAtLogsUrl = () => {
      const currentUrl = String(contents.getURL?.() || '').trim();
      return /^https:\/\/kie\.ai\/logs/i.test(currentUrl);
    };

    let domReadySeen = false;
    let uiProbeTimer = null;
    let bootstrapProbeTimeout = null;
    let uiProbeInFlight = false;
    let uiProbeCount = 0;
    let lastProbeLogAt = 0;
    let settled = false;
    let cleanup = () => {};

    const settleResolve = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve();
    };

    const settleReject = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };

    if (!isLoading()) {
      try {
        onProgress?.({
          step: 'window-load-skip',
          status: 'done',
          detail: 'Window is already loaded.',
          meta: { currentUrl: String(contents.getURL?.() || '').trim() }
        });
      } catch (_err) {}
      settleResolve();
      return;
    }

    let timeoutId = null;
    cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      if (domReadySettleTimer) {
        clearTimeout(domReadySettleTimer);
        domReadySettleTimer = null;
      }
      if (uiProbeTimer) {
        clearInterval(uiProbeTimer);
        uiProbeTimer = null;
      }
      if (bootstrapProbeTimeout) {
        clearTimeout(bootstrapProbeTimeout);
        bootstrapProbeTimeout = null;
      }
      contents.removeListener('did-finish-load', onFinish);
      contents.removeListener('did-fail-load', onFail);
      contents.removeListener('dom-ready', onDomReady);
      contents.removeListener('destroyed', onDestroyed);
    };

    const onFinish = () => {
      try {
        onProgress?.({
          step: 'window-load-finish',
          status: 'done',
          detail: 'did-finish-load received from hidden kie logs window.',
          meta: { currentUrl: String(contents.getURL?.() || '').trim() }
        });
      } catch (_err) {}
      settleResolve();
    };

    let domReadySettleTimer = null;
    const runUiProbe = async ({ forceLog = false } = {}) => {
      if (uiProbeInFlight || settled || !contents || contents.isDestroyed()) return;
      uiProbeInFlight = true;
      try {
        const probe = await probeKieLogsUiReady(contents);
        uiProbeCount += 1;
        const now = Date.now();
        const shouldLog = forceLog || now - lastProbeLogAt > 1400;
        if (shouldLog) {
          lastProbeLogAt = now;
          try {
            const s = probe?.signals || {};
            onProgress?.({
              step: 'window-ui-probe',
              status: probe?.ready ? 'done' : 'active',
              detail: probe?.ready
                ? 'Logs UI ready signals detected.'
                : 'Waiting for logs UI signals (pagination/rows/result/task hints).',
              meta: {
                probeCount: uiProbeCount,
                reason: String(probe?.reason || ''),
                pagination: !!s.pagination,
                goToInput: !!s.goToInput,
                visibleRows: Number(s.visibleRows || 0),
                resultButtons: Number(s.resultButtons || 0),
                taskHints: !!s.taskHints,
                hexTaskToken: !!s.hexTaskToken
              }
            });
          } catch (_err) {}
        }
        if (probe?.ready) {
          try {
            onProgress?.({
              step: 'window-ui-ready',
              status: 'done',
              detail: 'Proceeding because logs UI became usable (no full-load wait).',
              meta: {
                probeCount: uiProbeCount,
                signals: probe?.signals || {}
              }
            });
          } catch (_err) {}
          settleResolve();
        }
      } finally {
        uiProbeInFlight = false;
      }
    };

    const onDomReady = () => {
      domReadySeen = true;
      const currentUrl = String(contents.getURL?.() || '').trim();
      try {
        onProgress?.({
          step: 'window-dom-ready',
          status: 'done',
          detail: 'dom-ready received from hidden kie logs window.',
          meta: { currentUrl }
        });
      } catch (_err) {}

      if (!isAtLogsUrl()) return;
      startUiProbeLoop('dom-ready');
      if (domReadySettleTimer) clearTimeout(domReadySettleTimer);
      domReadySettleTimer = setTimeout(() => {
        if (!contents || contents.isDestroyed()) return;
        const finalUrl = String(contents.getURL?.() || '').trim();
        if (!/^https:\/\/kie\.ai\/logs/i.test(finalUrl)) return;
        try {
          onProgress?.({
            step: 'window-dom-ready-usable',
            status: 'done',
            detail: 'Using dom-ready logs page state without waiting for full load completion.',
            meta: { finalUrl, stillLoadingMainFrame: !!isLoading() }
          });
        } catch (_err) {}
        settleResolve();
      }, 320);
    };

    const startUiProbeLoop = (reason = 'unknown') => {
      if (settled) return;
      if (uiProbeTimer) return;
      try {
        onProgress?.({
          step: 'window-ui-probe-start',
          status: 'active',
          detail: `Starting logs UI probe loop (${reason}).`,
          meta: { reason }
        });
      } catch (_err) {}
      runUiProbe({ forceLog: true }).catch(() => {});
      uiProbeTimer = setInterval(() => {
        runUiProbe().catch(() => {});
      }, 260);
    };

    const onFail = (_event, errorCode, errorDescription, validatedURL, isMainFrame) => {
      if (isMainFrame === false) return;
      const desc = String(errorDescription || '').trim();
      const isAbort = Number(errorCode) === -3 || /ERR_ABORTED/i.test(desc);
      if (isAbort) {
        const currentUrl = String(contents.getURL?.() || '').trim();
        const atLogs = /^https:\/\/kie\.ai\/logs/i.test(currentUrl);
        if (atLogs && (!isLoading() || domReadySeen)) {
          try {
            onProgress?.({
              step: 'window-load-abort-resolved',
              status: 'done',
              detail: domReadySeen
                ? 'Received ERR_ABORTED; proceeding because logs page reached dom-ready.'
                : 'Received ERR_ABORTED but logs page is already ready.',
              meta: { currentUrl, domReadySeen }
            });
          } catch (_err) {}
          settleResolve();
          return;
        }
        if (atLogs) {
          startUiProbeLoop('abort-at-logs');
        }
        return;
      }
      try {
        onProgress?.({
          step: 'window-load-failed',
          status: 'error',
          detail: `did-fail-load (${errorCode}): ${String(errorDescription || 'Unknown error')}`,
          meta: { validatedURL: String(validatedURL || ''), errorCode: Number(errorCode || 0) }
        });
      } catch (_err) {}
      settleReject(new Error(`Failed to load ${validatedURL || KIE_LOGS_URL}: ${errorDescription || errorCode || 'error'}`));
    };

    const onDestroyed = () => {
      try {
        onProgress?.({
          step: 'window-destroyed',
          status: 'error',
          detail: 'Kie logs web contents was destroyed during load.'
        });
      } catch (_err) {}
      settleReject(new Error('Kie logs web contents was destroyed while loading.'));
    };

    contents.on('did-finish-load', onFinish);
    contents.on('did-fail-load', onFail);
    contents.on('dom-ready', onDomReady);
    contents.once('destroyed', onDestroyed);
    if (isAtLogsUrl()) {
      startUiProbeLoop('initial-at-logs');
    } else {
      bootstrapProbeTimeout = setTimeout(() => {
        if (settled) return;
        if (isAtLogsUrl()) {
          startUiProbeLoop('delayed-at-logs');
        }
      }, 220);
    }
    timeoutId = setTimeout(() => {
      const currentUrl = String(contents.getURL?.() || '').trim();
      const atLogs = /^https:\/\/kie\.ai\/logs/i.test(currentUrl);
      if (atLogs && domReadySeen) {
        try {
          onProgress?.({
            step: 'window-load-timeout-usable',
            status: 'done',
            detail: 'Timeout reached, but logs page is usable (dom-ready at /logs).',
            meta: { currentUrl, domReadySeen }
          });
        } catch (_err) {}
        settleResolve();
        return;
      }
      if (atLogs) {
        runUiProbe({ forceLog: true })
          .then(() => {
            if (settled) return;
            const nextUrl = String(contents.getURL?.() || '').trim();
            try {
              onProgress?.({
                step: 'window-load-timeout-soft',
                status: 'done',
                detail: 'Timeout hit; continuing with current logs page state and allowing query fallback.',
                meta: { currentUrl: nextUrl, domReadySeen, atLogs }
              });
            } catch (_err) {}
            settleResolve();
          })
          .catch(() => {
            if (settled) return;
            settleReject(new Error('Timed out loading kie.ai/logs.'));
          });
        return;
      }
      try {
        onProgress?.({
          step: 'window-load-timeout',
          status: 'error',
          detail: `Timed out waiting for kie.ai/logs after ${Math.max(5_000, Number(timeoutMs) || 45_000)}ms.`,
          meta: { currentUrl, domReadySeen, atLogs }
        });
      } catch (_err) {}
      settleReject(new Error('Timed out loading kie.ai/logs.'));
    }, Math.max(5_000, Number(timeoutMs) || 45_000));
  });

const isKieLoadAbortError = (err) => {
  const message = String(err?.message || err || '').trim();
  return /ERR_ABORTED|\(-3\)/i.test(message);
};

const ensureKieLogsWindowLoaded = async (win, { timeoutMs = 60_000, onProgress = null } = {}) => {
  if (!win || win.isDestroyed()) {
    throw new Error('Kie logs window is unavailable.');
  }
  const contents = win.webContents;
  if (!contents || contents.isDestroyed()) {
    throw new Error('Kie logs web contents is unavailable.');
  }
  const currentUrl = String(contents.getURL?.() || '').trim();
  const atLogs = /^https:\/\/kie\.ai\/logs/i.test(currentUrl);
  try {
    onProgress?.({
      step: 'window-check',
      status: 'active',
      detail: atLogs ? 'Hidden window is already at kie.ai/logs.' : 'Hidden window not at kie.ai/logs yet.',
      meta: { currentUrl }
    });
  } catch (_err) {}
  if (!atLogs) {
    try {
      onProgress?.({
        step: 'window-load-url',
        status: 'active',
        detail: `Loading URL in hidden window: ${KIE_LOGS_URL}`
      });
    } catch (_err) {}
    try {
      await win.loadURL(KIE_LOGS_URL);
      try {
        onProgress?.({
          step: 'window-load-dispatched',
          status: 'done',
          detail: 'loadURL dispatched successfully.'
        });
      } catch (_err) {}
    } catch (err) {
      if (!isKieLoadAbortError(err)) throw err;
      try {
        onProgress?.({
          step: 'window-load-aborted',
          status: 'active',
          detail: 'loadURL returned ERR_ABORTED, waiting for page readiness.'
        });
      } catch (_err) {}
    }
  }
  await waitForBrowserWindowLoad(win, { timeoutMs, onProgress });
  try {
    onProgress?.({
      step: 'window-ready',
      status: 'done',
      detail: 'Hidden kie logs window is ready.',
      meta: { finalUrl: String(contents.getURL?.() || '').trim() }
    });
  } catch (_err) {}
  return win;
};

const extractBearerTokenFromHeaders = (headers = {}) => {
  const keys = Object.keys(headers || {});
  for (const key of keys) {
    if (String(key || '').toLowerCase() !== 'authorization') continue;
    const raw = headers[key];
    const value = Array.isArray(raw) ? String(raw[0] || '') : String(raw || '');
    const match = value.match(/bearer\s+([A-Za-z0-9._-]{16,})/i);
    if (match && match[1]) return String(match[1]).trim();
  }
  return '';
};

const ensureKieLogsAuthCapture = () => {
  if (kieLogsAuthCaptureBound) return;
  const authSession = session.fromPartition(KIE_LOGS_AUTH_PARTITION);
  if (!authSession?.webRequest) return;
  authSession.webRequest.onBeforeSendHeaders(
    {
      urls: ['*://api.kie.ai/*', '*://kie.ai/*', '*://*.kie.ai/*']
    },
    (details, callback) => {
      try {
        const token = extractBearerTokenFromHeaders(details?.requestHeaders || {});
        if (token) {
          kieLogsLastBearerToken = token;
        }
      } catch (_err) {}
      callback({ requestHeaders: details.requestHeaders });
    }
  );
  kieLogsAuthCaptureBound = true;
};

const ensureKieLogsBridgeWindow = async ({ onProgress = null } = {}) => {
  ensureKieLogsAuthCapture();
  try {
    onProgress?.({
      step: 'auth-capture-ready',
      status: 'done',
      detail: 'Authorization header capture listener is ready.'
    });
  } catch (_err) {}
  if (kieLogsBridgeEnsureInFlight) {
    try {
      onProgress?.({
        step: 'bridge-wait-inflight',
        status: 'active',
        detail: 'A hidden window setup is already running. Waiting for it to finish.'
      });
    } catch (_err) {}
    return kieLogsBridgeEnsureInFlight;
  }
  kieLogsBridgeEnsureInFlight = (async () => {
    if (kieLogsBridgeWindow && !kieLogsBridgeWindow.isDestroyed()) {
      try {
        onProgress?.({
          step: 'bridge-reuse',
          status: 'active',
          detail: 'Reusing existing hidden kie logs window.'
        });
      } catch (_err) {}
      if (KIE_LOGS_BRIDGE_VISIBLE) {
        try {
          kieLogsBridgeWindow.show();
          kieLogsBridgeWindow.focus();
          onProgress?.({
            step: 'bridge-visible',
            status: 'done',
            detail: 'Bridge window is visible for live load/debug.'
          });
        } catch (_err) {}
      }
      await ensureKieLogsWindowLoaded(kieLogsBridgeWindow, { timeoutMs: 60_000, onProgress });
      return kieLogsBridgeWindow;
    }

    try {
      onProgress?.({
        step: 'bridge-create',
        status: 'active',
        detail: 'Creating hidden Electron window for kie.ai logs session.'
      });
    } catch (_err) {}
    const bridge = new BrowserWindow({
      show: !!KIE_LOGS_BRIDGE_VISIBLE,
      skipTaskbar: !KIE_LOGS_BRIDGE_VISIBLE,
      autoHideMenuBar: true,
      width: 1360,
      height: 920,
      webPreferences: {
        partition: KIE_LOGS_AUTH_PARTITION,
        contextIsolation: true,
        sandbox: true,
        nodeIntegration: false
      }
    });
    try {
      onProgress?.({
        step: 'bridge-created',
        status: 'done',
        detail: 'Electron bridge window created.'
      });
    } catch (_err) {}
    if (KIE_LOGS_BRIDGE_VISIBLE) {
      try {
        bridge.show();
        bridge.focus();
        onProgress?.({
          step: 'bridge-visible',
          status: 'done',
          detail: 'Bridge window is visible for live load/debug.'
        });
      } catch (_err) {}
    }
    bridge.on('closed', () => {
      if (kieLogsBridgeWindow === bridge) {
        kieLogsBridgeWindow = null;
      }
    });

    kieLogsBridgeWindow = bridge;
    await ensureKieLogsWindowLoaded(bridge, { timeoutMs: 60_000, onProgress });
    return bridge;
  })();
  try {
    return await kieLogsBridgeEnsureInFlight;
  } finally {
    kieLogsBridgeEnsureInFlight = null;
  }
};

const disposeKieLogsBridgeWindow = ({ reason = '' } = {}) => {
  const why = String(reason || '').trim();
  try {
    if (kieLogsBridgeWindow && !kieLogsBridgeWindow.isDestroyed()) {
      kieLogsBridgeWindow.destroy();
    }
  } catch (_err) {}
  kieLogsBridgeWindow = null;
  if (why) {
    console.warn('[KieLogs] bridge:disposed', { reason: why });
  }
};

const normalizeKieLogsQueryPayload = (payload = {}) => {
  const toInt = (value, fallback) => {
    const parsed = Number.parseInt(String(value ?? ''), 10);
    if (!Number.isFinite(parsed)) return fallback;
    return parsed;
  };
  const pageNum = Math.max(1, toInt(payload.pageNum, 1));
  const pageSize = Math.max(1, Math.min(100, toInt(payload.pageSize, 10)));
  const maxScrollSteps = Math.max(40, Math.min(400, toInt(payload.maxScrollSteps, 220)));
  const waitMs = Math.max(120, Math.min(1600, toInt(payload.waitMs, 280)));
  const fastRowsOnly = payload?.fastRowsOnly !== false;

  return {
    pageNum,
    pageSize,
    maxScrollSteps,
    waitMs,
    fastRowsOnly
  };
};

const runKieLogsQueryInWebContents = async (webContents, payload = {}, { sender = null } = {}) => {
  if (!webContents || webContents.isDestroyed()) {
    throw new Error('Kie logs web contents is unavailable.');
  }
  const safePayload = normalizeKieLogsQueryPayload(payload);
  const startedAt = Date.now();
  console.info('[KieLogs] query:start', safePayload);
  const scriptPayload = {
    ...safePayload,
    seedBearerToken: String(kieLogsLastBearerToken || '').trim()
  };
  const script = `(() => {
    const input = ${JSON.stringify(scriptPayload)};
    const pageNum = Math.max(1, Number(input.pageNum) || 1);
    const pageSize = Math.max(1, Number(input.pageSize) || 10);
    const targetCount = (pageNum + 1) * pageSize;
    const maxScrollSteps = Math.max(40, Number(input.maxScrollSteps) || 220);
    const waitMs = Math.max(120, Number(input.waitMs) || 280);
    const fastRowsOnly = !!input.fastRowsOnly;
    const debug = [];
    const GLOBAL_DEBUG_KEY = '__kieScrapeDebugBuffer';
    try {
      window[GLOBAL_DEBUG_KEY] = [];
    } catch (_err) {}
    const pushDebug = (event, details = {}) => {
      try {
        const entry = {
          at: Date.now(),
          event: String(event || '').slice(0, 120)
        };
        if (details && typeof details === 'object') {
          entry.details = details;
          Object.assign(entry, details);
        }
        debug.push(entry);
        if (debug.length > 240) debug.shift();
        try {
          if (!Array.isArray(window[GLOBAL_DEBUG_KEY])) window[GLOBAL_DEBUG_KEY] = [];
          window[GLOBAL_DEBUG_KEY].push(entry);
          if (window[GLOBAL_DEBUG_KEY].length > 420) window[GLOBAL_DEBUG_KEY].shift();
        } catch (_err) {}
        try {
          console.info('[KieScrape] ' + JSON.stringify(entry));
        } catch (_err) {}
      } catch (_err) {}
    };
    pushDebug('start', { pageNum, pageSize, maxScrollSteps, waitMs, fastRowsOnly });

    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const toText = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
    const isHttpUrl = (value) => /^https?:\\/\\//i.test(String(value || '').trim());
    const isMediaLikeUrl = (value) => {
      const text = String(value || '').toLowerCase().trim();
      if (!text || !isHttpUrl(text)) return false;
      if (/\\.(png|jpe?g|webp|avif|gif|bmp|svg|mp4|mov|webm|mkv|m4v|mp3|wav|ogg|m4a)(\\?|$)/i.test(text)) {
        return true;
      }
      return /(tempfile\\.aiquickdraw|file\\.a|cdn|oss|result|output|image|video|media|download)/i.test(text);
    };
    const mediaPriority = (value) => {
      const text = String(value || '').toLowerCase().trim();
      if (!text) return -999;
      let score = 0;
      if (/tempfile\\.aiquickdraw/.test(text)) score += 70;
      if (/(result|output|download|generated|preview|display|show)/.test(text)) score += 35;
      if (/(image|img|video|media|oss|cdn|file\\.)/.test(text)) score += 10;
      if (/(input|source|reference|original|upload|prompt)/.test(text)) score -= 28;
      if (/(logo|icon|favicon|avatar|cookie|tracking|analytics|clarity|google)/.test(text)) score -= 45;
      return score;
    };
    const isInputLikeUrl = (value) =>
      /(input|source|reference|original|upload|prompt|mask|control|init|thumbnail|thumb|origin)/i.test(
        String(value || '').toLowerCase()
      );
    const sortMediaUrls = (urls = []) =>
      Array.from(new Set(Array.isArray(urls) ? urls : []))
        .filter((url) => isMediaLikeUrl(url))
        .sort((a, b) => mediaPriority(b) - mediaPriority(a));
    const hashText = (value) => {
      const text = String(value || '');
      let hash = 0;
      for (let i = 0; i < text.length; i += 1) {
        hash = (hash * 31 + text.charCodeAt(i)) >>> 0;
      }
      return String(hash);
    };

    const extractUrlsFromText = (text = '') => {
      const out = [];
      const seen = new Set();
      const matches = String(text || '').match(/https?:\\/\\/[^\\s"'<>]+/gi) || [];
      matches.forEach((match) => {
        const cleaned = String(match || '').replace(/[),.;]+$/g, '').trim();
        if (!cleaned || !isMediaLikeUrl(cleaned) || seen.has(cleaned)) return;
        seen.add(cleaned);
        out.push(cleaned);
      });
      return out;
    };

    const extractUrlsFromElement = (el) => {
      const out = [];
      const seen = new Set();
      const push = (raw) => {
        const text = toText(raw);
        if (!text) return;
        if (isMediaLikeUrl(text) && !seen.has(text)) {
          seen.add(text);
          out.push(text);
        }
        extractUrlsFromText(text).forEach((url) => {
          if (!seen.has(url)) {
            seen.add(url);
            out.push(url);
          }
        });
      };

      ['src', 'href', 'poster', 'data-src', 'data-original', 'data-url', 'data-image', 'data-media'].forEach(
        (attr) => {
          try {
            el.querySelectorAll('[' + attr + ']').forEach((node) => push(node.getAttribute(attr)));
          } catch (_err) {}
        }
      );
      try {
        el.querySelectorAll('source[src]').forEach((node) => push(node.getAttribute('src')));
      } catch (_err) {}
      try {
        el.querySelectorAll('[srcset]').forEach((node) => {
          const srcset = String(node.getAttribute('srcset') || '');
          srcset.split(',').forEach((part) => {
            const first = toText(part).split(/\\s+/)[0] || '';
            push(first);
          });
        });
      } catch (_err) {}

      try {
        el.querySelectorAll('*').forEach((node) => {
          const inline = String((node.getAttribute && node.getAttribute('style')) || '');
          if (!inline || !/background-image/i.test(inline)) return;
          const re = /url\\((['"]?)(.*?)\\1\\)/gi;
          let match = null;
          while ((match = re.exec(inline))) {
            if (match && match[2]) push(match[2]);
          }
        });
      } catch (_err) {}

      push(el.innerText || el.textContent || '');
      return sortMediaUrls(out);
    };

    const normalizeTaskIdCandidate = (value = '') => {
      const text = toText(value);
      if (!text) return '';

      const hexMatches = text.toLowerCase().match(/[0-9a-f]{24,64}/g) || [];
      if (hexMatches.length) {
        const counts = new Map();
        hexMatches.forEach((hex) => counts.set(hex, (counts.get(hex) || 0) + 1));
        const sorted = [...counts.entries()].sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return String(b[0] || '').length - String(a[0] || '').length;
        });
        const bestHex = toText(sorted[0]?.[0] || '');
        if (bestHex) return bestHex;
      }

      const taskPrefixed = text.match(/\\b(task[_-]?[A-Za-z0-9_-]{6,})\\b/i);
      if (taskPrefixed && taskPrefixed[1]) {
        const candidate = toText(taskPrefixed[1]);
        if (candidate && !/^task-\\d{10,}$/i.test(candidate)) return candidate;
      }

      const compact = text.replace(/^[^A-Za-z0-9_-]+|[^A-Za-z0-9_-]+$/g, '');
      if (!compact) return '';
      if (compact.length < 6 || compact.length > 140) return '';
      if (/\\s/.test(compact)) return '';
      if (!/^[A-Za-z0-9_-]+$/.test(compact)) return '';
      if (/^task-\\d{10,}$/i.test(compact)) return '';
      return compact;
    };

    const extractTaskId = (text = '', el = null) => {
      const source = toText(text);
      const patterns = [
        /task\\s*id\\s*[:#]?\\s*([A-Za-z0-9_-]{6,})/i,
        /\\b([0-9a-f]{24,})\\b/i,
        /\\b([A-Za-z0-9]{8,}-[A-Za-z0-9_-]{8,})\\b/i
      ];
      for (const pattern of patterns) {
        const match = source.match(pattern);
        if (match && match[1]) {
          const normalized = normalizeTaskIdCandidate(match[1]);
          if (normalized) return normalized;
        }
      }
      if (el) {
        const attrHints = ['data-key', 'data-id', 'data-row-key', 'data-task-id', 'id'];
        for (const attr of attrHints) {
          const candidate = normalizeTaskIdCandidate(el.getAttribute ? el.getAttribute(attr) : '');
          if (candidate.length >= 6) return candidate;
        }
      }
      return normalizeTaskIdCandidate(source);
    };

    const extractStatus = (text = '') => {
      const source = toText(text).toLowerCase();
      if (!source) return 'unknown';
      if (/(success|succeeded|completed|done)/i.test(source)) return 'success';
      if (/(fail|error|failed|denied)/i.test(source)) return 'fail';
      if (/(processing|running|generating|waiting|queue|pending)/i.test(source)) return 'processing';
      return 'unknown';
    };

    const extractTimeMs = (text = '') => {
      const source = toText(text);
      if (!source) return 0;
      const patterns = [
        /(20\\d{2}[\\/\\-]\\d{1,2}[\\/\\-]\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2})/,
        /(20\\d{2}[\\/\\-]\\d{1,2}[\\/\\-]\\d{1,2}\\s+\\d{1,2}:\\d{2})/
      ];
      for (const pattern of patterns) {
        const match = source.match(pattern);
        if (!match || !match[1]) continue;
        const normalized = match[1].replace(/\\//g, '-');
        const parsed = Date.parse(normalized);
        if (Number.isFinite(parsed)) return parsed;
      }
      return 0;
    };

    const extractPrompt = (el, text = '') => {
      const source = toText(text);
      const lower = source.toLowerCase();
      const promptIdx = lower.indexOf('prompt');
      if (promptIdx >= 0) {
        const tail = source.slice(promptIdx).replace(/^prompt\\s*:?\\s*/i, '').trim();
        if (tail.length >= 8) return tail.slice(0, 1200);
      }
      const lines = source
        .split(/\\n+/)
        .map((line) => toText(line))
        .filter(Boolean)
        .filter((line) => !/^task\\s*id\\b/i.test(line))
        .filter((line) => !/^(success|processing|waiting|queue|fail|failed)\\b/i.test(line));
      const longLine = lines.find((line) => line.length >= 25);
      if (longLine) return longLine.slice(0, 1200);
      const title = toText(el?.querySelector?.('img')?.getAttribute?.('alt') || '');
      return title.slice(0, 1200);
    };

    const collectUrlsFromAny = (...values) => {
      const out = [];
      const seen = new Set();
      const queue = [...values];
      while (queue.length) {
        const current = queue.shift();
        if (!current) continue;
        if (typeof current === 'string') {
          const trimmed = current.trim();
          if (!trimmed) continue;
          const embedded = trimmed.match(/https?:\\/\\/[^\\s"'<>]+/gi) || [];
          embedded.forEach((candidate) => {
            const normalized = toText(candidate).replace(/[),.;]+$/g, '');
            if (!normalized || !isMediaLikeUrl(normalized) || seen.has(normalized)) return;
            seen.add(normalized);
            out.push(normalized);
          });
          if (isMediaLikeUrl(trimmed) && !seen.has(trimmed)) {
            seen.add(trimmed);
            out.push(trimmed);
          }
          if (trimmed[0] === '{' || trimmed[0] === '[') {
            try {
              queue.push(JSON.parse(trimmed));
            } catch (_err) {}
          }
          continue;
        }
        if (Array.isArray(current)) {
          queue.push(...current);
          continue;
        }
        if (typeof current === 'object') {
          queue.push(
            current.resultUrls,
            current.result_urls,
            current.urls,
            current.mediaUrls,
            current.media_urls,
            current.downloadUrl,
            current.download_url,
            current.url,
            current.data,
            current.output,
            current.result,
            current.resultJson,
            current.result_json,
            ...Object.values(current)
          );
        }
      }
      return sortMediaUrls(out);
    };

    const isTempfileUrl = (value) => /https?:\\/\\/tempfile\\.aiquickdraw\\.com\\//i.test(String(value || '').trim());

    const ensureNetworkMediaCapture = () => {
      const KEY = '__nb_kie_network_media_capture_v1';
      if (window[KEY] && typeof window[KEY].snapshot === 'function') {
        return window[KEY];
      }
      const state = {
        entries: [],
        entrySet: new Set(),
        taskMap: new Map(),
        maxEntries: 2400
      };
      const normalizeUrl = (raw) => toText(raw).replace(/[),.;]+$/g, '');
      const toTaskId = (raw) => normalizeTaskIdCandidate(raw);
      const extractTaskIdsFromText = (raw = '') => {
        const source = String(raw || '');
        if (!source) return [];
        const out = new Set();
        const patterns = [
          /(?:taskId|task_id)\\s*[:=]\\s*["']?([A-Za-z0-9_-]{6,})/gi,
          /[?&]taskId=([A-Za-z0-9_-]{6,})/gi,
          /\\btask\\s*id\\s*[:#]?\\s*([A-Za-z0-9_-]{6,})/gi
        ];
        patterns.forEach((pattern) => {
          let match = null;
          while ((match = pattern.exec(source))) {
            if (match && match[1]) out.add(toTaskId(match[1]));
          }
        });
        const fallback = extractTaskId(source);
        if (fallback) out.add(toTaskId(fallback));
        return Array.from(out).filter(Boolean);
      };
      const trackUrl = (rawUrl, taskId = '', source = '') => {
        const url = normalizeUrl(rawUrl);
        if (!url || !isMediaLikeUrl(url)) return;
        const safeTaskId = toTaskId(taskId);
        const key = safeTaskId ? safeTaskId + '|' + url : '|' + url;
        if (state.entrySet.has(key)) return;
        state.entrySet.add(key);
        state.entries.push({
          url,
          taskId: safeTaskId,
          source: toText(source),
          at: Date.now()
        });
        if (safeTaskId) {
          if (!state.taskMap.has(safeTaskId)) state.taskMap.set(safeTaskId, new Set());
          state.taskMap.get(safeTaskId).add(url);
        }
        if (state.entries.length > state.maxEntries) {
          const removed = state.entries.splice(0, state.entries.length - state.maxEntries);
          removed.forEach((item) => {
            const removedKey = (item.taskId ? item.taskId : '') + '|' + item.url;
            state.entrySet.delete(removedKey);
          });
        }
      };
      const captureFromAny = (value, hintTaskId = '', source = '') => {
        const hinted = toTaskId(hintTaskId);
        const taskIds = new Set(hinted ? [hinted] : []);
        const queue = [value];
        let safety = 0;
        while (queue.length && safety < 600) {
          safety += 1;
          const current = queue.shift();
          if (!current) continue;
          if (typeof current === 'string') {
            const text = String(current);
            extractTaskIdsFromText(text).forEach((taskId) => taskIds.add(taskId));
            extractUrlsFromText(text).forEach((url) => {
              if (taskIds.size) {
                taskIds.forEach((taskId) => trackUrl(url, taskId, source));
              } else {
                trackUrl(url, '', source);
              }
            });
            if (text[0] === '{' || text[0] === '[') {
              try {
                queue.push(JSON.parse(text));
              } catch (_err) {}
            }
            continue;
          }
          if (Array.isArray(current)) {
            current.forEach((item) => queue.push(item));
            continue;
          }
          if (typeof current === 'object') {
            queue.push(...Object.values(current));
          }
        }
      };
      const snapshot = () => new Set(state.entries.map((entry) => entry.url));
      const diff = (beforeSet = new Set(), taskId = '') => {
        const safeTaskId = toTaskId(taskId);
        const out = [];
        const seen = new Set();
        const push = (raw) => {
          const url = normalizeUrl(raw);
          if (!url || seen.has(url) || (beforeSet && beforeSet.has(url))) return;
          seen.add(url);
          out.push(url);
        };
        if (safeTaskId && state.taskMap.has(safeTaskId)) {
          state.taskMap.get(safeTaskId).forEach((url) => push(url));
        }
        for (let i = state.entries.length - 1; i >= 0; i -= 1) {
          const entry = state.entries[i];
          if (!entry || !entry.url) continue;
          if (safeTaskId && entry.taskId && entry.taskId !== safeTaskId) continue;
          if (safeTaskId && !entry.taskId && !isTempfileUrl(entry.url)) continue;
          push(entry.url);
        }
        return sortMediaUrls(out);
      };
      const byTask = (taskId = '') => {
        const safeTaskId = toTaskId(taskId);
        if (!safeTaskId || !state.taskMap.has(safeTaskId)) return [];
        return sortMediaUrls(Array.from(state.taskMap.get(safeTaskId)));
      };
      const api = {
        snapshot,
        diff,
        byTask,
        captureFromAny,
        trackUrl,
        state
      };

      if (!window.__nb_kie_capture_fetch_patched && typeof window.fetch === 'function') {
        const nativeFetch = window.fetch.bind(window);
        window.fetch = async (...args) => {
          const requestRef = args[0];
          const requestUrl = toText(
            typeof requestRef === 'string'
              ? requestRef
              : requestRef?.url || ''
          );
          const bodyHint = toText(args?.[1]?.body || '');
          const taskHint = extractTaskId(requestUrl) || extractTaskId(bodyHint);
          try {
            api.captureFromAny(requestUrl, taskHint, 'fetch-request');
            if (bodyHint) api.captureFromAny(bodyHint, taskHint, 'fetch-body');
          } catch (_err) {}
          const response = await nativeFetch(...args);
          try {
            api.captureFromAny(response?.url || '', taskHint, 'fetch-response-url');
            const contentType = toText(response?.headers?.get?.('content-type') || '').toLowerCase();
            if (/json|text|javascript|xml/.test(contentType) || /result|task|record|job/i.test(requestUrl)) {
              const text = await response.clone().text().catch(() => '');
              if (text) api.captureFromAny(text, taskHint, 'fetch-response');
            }
          } catch (_err) {}
          return response;
        };
        window.__nb_kie_capture_fetch_patched = true;
      }

      if (!window.__nb_kie_capture_xhr_patched && window.XMLHttpRequest?.prototype) {
        const proto = window.XMLHttpRequest.prototype;
        const nativeOpen = proto.open;
        const nativeSend = proto.send;
        proto.open = function(method, url, ...rest) {
          this.__nb_kie_capture_url = toText(url || '');
          this.__nb_kie_capture_method = toText(method || '');
          return nativeOpen.call(this, method, url, ...rest);
        };
        proto.send = function(body) {
          const requestUrl = toText(this.__nb_kie_capture_url || '');
          const bodyHint = toText(body || '');
          const taskHint = extractTaskId(requestUrl) || extractTaskId(bodyHint);
          try {
            api.captureFromAny(requestUrl, taskHint, 'xhr-request');
            if (bodyHint) api.captureFromAny(bodyHint, taskHint, 'xhr-body');
          } catch (_err) {}
          try {
            this.addEventListener(
              'loadend',
              () => {
                try {
                  const responseUrl = toText(this.responseURL || requestUrl || '');
                  api.captureFromAny(responseUrl, taskHint, 'xhr-response-url');
                  const contentType = toText(this.getResponseHeader?.('content-type') || '').toLowerCase();
                  if (/json|text|javascript|xml/.test(contentType) || /result|task|record|job/i.test(requestUrl)) {
                    const text = typeof this.responseText === 'string' ? this.responseText : '';
                    if (text) api.captureFromAny(text, taskHint, 'xhr-response');
                  }
                } catch (_err) {}
              },
              { once: true }
            );
          } catch (_err) {}
          return nativeSend.call(this, body);
        };
        window.__nb_kie_capture_xhr_patched = true;
      }

      window[KEY] = api;
      return api;
    };

    const networkMediaCapture = ensureNetworkMediaCapture();

    const collectTempfileUrlsFromDocument = () => {
      const out = [];
      const seen = new Set();
      const push = (raw) => {
        const url = toText(raw).replace(/[),.;]+$/g, '');
        if (!url || !isTempfileUrl(url) || seen.has(url)) return;
        seen.add(url);
        out.push(url);
      };
      try {
        document
          .querySelectorAll(
            'a[href*="tempfile.aiquickdraw"],img[src*="tempfile.aiquickdraw"],video[src*="tempfile.aiquickdraw"],source[src*="tempfile.aiquickdraw"],[data-url*="tempfile.aiquickdraw"],[data-src*="tempfile.aiquickdraw"]'
          )
          .forEach((el) => {
            push(el.getAttribute && el.getAttribute('href'));
            push(el.getAttribute && el.getAttribute('src'));
            push(el.getAttribute && el.getAttribute('data-url'));
            push(el.getAttribute && el.getAttribute('data-src'));
            push(el.currentSrc || '');
          });
      } catch (_err) {}
      try {
        const html = String(document.documentElement?.outerHTML || '');
        const matches = html.match(/https?:\\/\\/tempfile\\.aiquickdraw\\.com\\/images\\/[^"'\\s<>]+/gi) || [];
        matches.forEach((url) => push(url));
      } catch (_err) {}
      return sortMediaUrls(out);
    };

    const collectAuthTokenCandidates = () => {
      const tokenSet = new Set();
      const pushToken = (raw) => {
        const text = toText(raw);
        if (!text) return;
        const bearerMatch = text.match(/bearer\\s+([A-Za-z0-9._-]{12,})/i);
        if (bearerMatch && bearerMatch[1]) {
          tokenSet.add(bearerMatch[1]);
          return;
        }
        if (/^https?:\\/\\//i.test(text)) return;
        if (/^[A-Za-z0-9._-]{24,}$/.test(text)) {
          tokenSet.add(text);
        }
      };
      pushToken(input.seedBearerToken || '');
      const readStorage = (storage) => {
        if (!storage) return;
        try {
          for (let i = 0; i < storage.length; i += 1) {
            const key = String(storage.key(i) || '');
            const value = String(storage.getItem(key) || '');
            if (!value) continue;
            if (/token|auth|access|bearer|jwt|session/i.test(key)) {
              pushToken(value);
            }
            try {
              const parsed = JSON.parse(value);
              const stack = [parsed];
              while (stack.length) {
                const item = stack.pop();
                if (!item) continue;
                if (typeof item === 'string') {
                  pushToken(item);
                } else if (Array.isArray(item)) {
                  stack.push(...item);
                } else if (typeof item === 'object') {
                  stack.push(...Object.values(item));
                }
              }
            } catch (_err) {}
          }
        } catch (_err) {}
      };
      readStorage(window.localStorage);
      readStorage(window.sessionStorage);
      try {
        const cookieRaw = String(document.cookie || '');
        cookieRaw
          .split(';')
          .map((part) => part.trim())
          .filter(Boolean)
          .forEach((pair) => {
            const idx = pair.indexOf('=');
            const key = idx >= 0 ? pair.slice(0, idx).trim() : pair.trim();
            const value = idx >= 0 ? pair.slice(idx + 1).trim() : '';
            if (!value) return;
            if (/token|auth|access|bearer|jwt|session/i.test(key)) {
              try {
                pushToken(decodeURIComponent(value));
              } catch (_err) {
                pushToken(value);
              }
            }
          });
      } catch (_err) {}
      return Array.from(tokenSet).slice(0, 8);
    };

    const fetchRecordInfoByTaskId = async (taskId, tokenCandidates = []) => {
      const safeTaskId = toText(taskId);
      if (!safeTaskId) return null;
      const encodedTaskId = encodeURIComponent(safeTaskId);
      const endpoints = [
        'https://api.kie.ai/api/v1/jobs/recordInfo?taskId=' + encodedTaskId,
        'https://kie.ai/api/v1/jobs/recordInfo?taskId=' + encodedTaskId,
        '/api/v1/jobs/recordInfo?taskId=' + encodedTaskId
      ];
      const headersList = [{}];
      tokenCandidates.forEach((token) => {
        const clean = toText(token);
        if (!clean) return;
        headersList.push({ Authorization: 'Bearer ' + clean });
      });

      for (const endpoint of endpoints) {
        for (const headers of headersList) {
          try {
            const response = await fetch(endpoint, {
              method: 'GET',
              credentials: 'include',
              mode: 'cors',
              cache: 'no-store',
              headers
            });
            const data = await response.json().catch(() => null);
            if (!data) continue;
            const validCode = Number(data.code) === 200 || Number(data.code) === 0 || data.success === true;
            if (validCode && data.data) return data.data;
            if (data.taskId || data.resultJson || data.resultUrls || data.output) return data;
          } catch (_err) {}
        }
      }
      return null;
    };

    const enrichRowsFromTaskApi = async (recordMap, limit = 20) => {
      const targets = Array.from(recordMap.entries())
        .filter(([, row]) => {
          const taskId = toText(row?.taskId);
          return !!taskId;
        })
        .slice(0, Math.max(1, Number(limit) || 1));
      if (!targets.length) return;

      const tokenCandidates = collectAuthTokenCandidates();
      const runTarget = async ([key, row]) => {
          const info = await fetchRecordInfoByTaskId(row.taskId, tokenCandidates);
          if (!info) return null;
          const inputUrlSet = new Set(
            collectUrlsFromAny(
              info.param,
              info.params,
              info.paramJson,
              info.param_json,
              info.input,
              info.inputs,
              info.source,
              info.sourceImage,
              info.sourceImages,
              info.reference,
              info.referenceImage,
              info.referenceImages
            ).map((url) => toText(url))
          );
          const keepOutputUrls = (urls = []) => {
            const ranked = sortMediaUrls(Array.isArray(urls) ? urls : []);
            const filtered = ranked.filter((url) => {
              const normalized = toText(url);
              if (!normalized) return false;
              if (inputUrlSet.has(normalized)) return false;
              return !isInputLikeUrl(normalized);
            });
            return filtered.length ? filtered : ranked;
          };
          const resultUrls = keepOutputUrls(collectUrlsFromAny(
            info.resultJson,
            info.result,
            info.resultUrls,
            info.result_urls,
            info.output
          ));
          const extraUrls = keepOutputUrls(collectUrlsFromAny(
            info.urls,
            info.mediaUrls,
            info.media_urls,
            info.resultJson,
            info.resultUrls
          ));
          return {
            key,
            state: toText(info.state).toLowerCase() || 'unknown',
            model: toText(info.model),
            createTime: Number(info.createTime || 0),
            updateTime: Number(info.updateTime || 0),
            completeTime: Number(info.completeTime || 0),
            failMsg: toText(info.failMsg),
            failCode: toText(info.failCode),
            prompt: toText((() => {
              try {
                const parsed = typeof info.param === 'string' ? JSON.parse(info.param || '{}') : info.param || {};
                return parsed?.input?.prompt || parsed?.prompt || '';
              } catch (_err) {
                return '';
              }
            })()),
            resultUrls,
            urls: resultUrls.length ? resultUrls : extraUrls
          };
      };
      const settled = [];
      const concurrency = Math.max(2, Math.min(12, Number(limit) > 120 ? 12 : 8));
      for (let i = 0; i < targets.length; i += concurrency) {
        const chunk = targets.slice(i, i + concurrency);
        const chunkSettled = await Promise.allSettled(chunk.map((entry) => runTarget(entry)));
        settled.push(...chunkSettled);
      }

      settled.forEach((entry) => {
        if (entry.status !== 'fulfilled' || !entry.value?.key) return;
        const patch = entry.value;
        const record = recordMap.get(patch.key);
        if (!record) return;
        const mergedResult = sortMediaUrls([...(record.resultUrls || []), ...(patch.resultUrls || [])]);
        const mergedUrls = sortMediaUrls([...(record.urls || []), ...(patch.urls || []), ...mergedResult]);
        record.state = patch.state || record.state || 'unknown';
        record.model = patch.model || record.model || '';
        if (Number(patch.createTime || 0) > 0) {
          record.createTime = Number(patch.createTime || 0);
        } else {
          record.createTime = Number(record.createTime || 0);
        }
        record.updateTime = Math.max(Number(record.updateTime || 0), Number(patch.updateTime || 0));
        record.completeTime = Math.max(Number(record.completeTime || 0), Number(patch.completeTime || 0));
        record.failMsg = patch.failMsg || record.failMsg || '';
        record.failCode = patch.failCode || record.failCode || '';
        record.prompt = patch.prompt || record.prompt || '';
        record.resultUrls = mergedResult;
        record.urls = mergedResult.length ? mergedResult : mergedUrls;
        recordMap.set(patch.key, record);
      });
    };

    const getRowCandidates = () => {
      const set = new Set();
      const selectors = [
        'table tbody tr',
        '[role="row"]',
        '[data-slot="tbody"] tr',
        'tbody tr',
        '[data-row-key]',
        '[data-key]',
        '[class*="table-row"]',
        '[class*="TableRow"]'
      ];
      selectors.forEach((selector) => {
        try {
          document.querySelectorAll(selector).forEach((el) => set.add(el));
        } catch (_err) {}
      });
      const raw = Array.from(set).filter((el) => {
        const text = toText(el.innerText || el.textContent || '');
        if (!text || text.length < 18) return false;
        if (/^no\\s+logs?\\b/i.test(text) || /^no\\s+records?\\b/i.test(text)) return false;
        const hasDate = /\\d{4}[\\/\\-]\\d{1,2}[\\/\\-]\\d{1,2}/.test(text);
        const hasState = /(success|processing|waiting|queue|fail|failed|running)/i.test(text);
        const hasTaskHint = /task\\s*id/i.test(text);
        if (!hasDate && !hasState && !hasTaskHint) return false;
        return true;
      });
      const topLevelRaw = raw.filter((el) => !raw.some((other) => other !== el && el.contains(other)));
      const visibleRows = topLevelRaw.filter((el) => isElementVisible(el) || isElementVisible(el.parentElement));
      if (visibleRows.length) return visibleRows;
      return topLevelRaw;
    };

    const isElementVisible = (el) => {
      if (!el || typeof el.getBoundingClientRect !== 'function') return false;
      const rect = el.getBoundingClientRect();
      if (!rect || rect.width < 2 || rect.height < 2) return false;
      if (rect.bottom < 0 || rect.right < 0) return false;
      const style = getComputedStyle(el);
      if (!style) return false;
      if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || 1) === 0) return false;
      return true;
    };

    const collectVisibleDocumentMediaUrls = () => {
      const out = [];
      const seen = new Set();
      const push = (raw) => {
        const text = toText(raw);
        if (!text || !isMediaLikeUrl(text)) return;
        if (/(favicon|logo|icon|avatar|clarity|cookieyes|googletagmanager)/i.test(text)) return;
        if (seen.has(text)) return;
        seen.add(text);
        out.push(text);
      };

      try {
        document.querySelectorAll('img,video,source,a[href],[data-src],[data-original],[data-url],[poster]').forEach((el) => {
          const visible = isElementVisible(el) || isElementVisible(el.parentElement);
          if (!visible) return;
          push(el.getAttribute && el.getAttribute('src'));
          push(el.getAttribute && el.getAttribute('href'));
          push(el.getAttribute && el.getAttribute('poster'));
          push(el.getAttribute && el.getAttribute('data-src'));
          push(el.getAttribute && el.getAttribute('data-original'));
          push(el.getAttribute && el.getAttribute('data-url'));
          push(el.currentSrc || '');
        });
      } catch (_err) {}

      try {
        const overlays = document.querySelectorAll(
          '[role="tooltip"],[role="dialog"],[data-slot="content"],[class*="popover"],[class*="tooltip"]'
        );
        overlays.forEach((el) => {
          if (!isElementVisible(el)) return;
          extractUrlsFromText(el.innerText || el.textContent || '').forEach((url) => push(url));
          extractUrlsFromElement(el).forEach((url) => push(url));
        });
      } catch (_err) {}

      return sortMediaUrls(out);
    };

    const resolveRowRecordKey = (row, recordMap) => {
      const text = toText(row?.innerText || row?.textContent || '');
      if (!text) return '';
      const taskId = extractTaskId(text, row);
      if (taskId) {
        if (recordMap.has(taskId)) return taskId;
        for (const [key, rowData] of recordMap.entries()) {
          if (String(rowData?.taskId || '').trim() === taskId) return key;
        }
      }
      const candidateKeys = [
        toText(row.getAttribute ? row.getAttribute('data-key') : ''),
        toText(row.getAttribute ? row.getAttribute('data-id') : ''),
        hashText(text + '|'),
        hashText(text)
      ].filter(Boolean);
      for (const key of candidateKeys) {
        if (recordMap.has(key)) return key;
      }
      return '';
    };

    const enrichRowsFromResultButtons = async (rows, recordMap, limit = 10, timeBudgetMs = 3200) => {
      const startedAt = Date.now();
      const targets = [];
      for (const row of rows) {
        if (Date.now() - startedAt > Math.max(600, Number(timeBudgetMs) || 3200)) break;
        const key = resolveRowRecordKey(row, recordMap);
        if (!key) continue;
        const data = recordMap.get(key);
        if (!data) continue;
        const currentResults = Array.isArray(data.resultUrls) ? data.resultUrls : [];
        if (currentResults.length) continue;
        targets.push({ row, key, data });
        if (targets.length >= limit) break;
      }
      for (const target of targets) {
        if (Date.now() - startedAt > Math.max(600, Number(timeBudgetMs) || 3200)) break;
        const { row, key } = target;
        const record = recordMap.get(key);
        if (!record) continue;
        const baseRowUrls = new Set(
          sortMediaUrls([
            ...extractUrlsFromElement(row),
            ...(Array.isArray(record.urls) ? record.urls : [])
          ])
        );
        const controls = [];
        try {
          row.querySelectorAll('button,[role="button"],a,span,div').forEach((node) => {
            const label = toText(
              (node.getAttribute && (node.getAttribute('aria-label') || node.getAttribute('title'))) ||
                node.innerText ||
                node.textContent ||
                ''
            ).toLowerCase();
            if (!label || !/result/.test(label)) return;
            if (!controls.includes(node)) controls.push(node);
          });
        } catch (_err) {}
        if (!controls.length) {
          const rowRect = row.getBoundingClientRect ? row.getBoundingClientRect() : null;
          const fallback = [];
          try {
            row.querySelectorAll('button,[role="button"],a').forEach((node) => {
              const rect = node.getBoundingClientRect ? node.getBoundingClientRect() : null;
              if (!rect || rect.width < 6 || rect.height < 6) return;
              if (rowRect && rect.left < rowRect.left + rowRect.width * 0.45) return;
              fallback.push(node);
            });
          } catch (_err) {}
          fallback
            .sort((a, b) => {
              const ar = a.getBoundingClientRect ? a.getBoundingClientRect() : { left: 0 };
              const br = b.getBoundingClientRect ? b.getBoundingClientRect() : { left: 0 };
              return Number(br.left || 0) - Number(ar.left || 0);
            })
            .forEach((node) => {
              if (!controls.includes(node)) controls.push(node);
            });
        }
        for (const ctrl of controls.slice(0, 2)) {
          if (Date.now() - startedAt > Math.max(600, Number(timeBudgetMs) || 3200)) break;
          const before = new Set(collectVisibleDocumentMediaUrls());
          const beforeCaptured = networkMediaCapture.snapshot();
          try {
            if (typeof ctrl.scrollIntoView === 'function') {
              ctrl.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
            }
            if (typeof ctrl.focus === 'function') ctrl.focus({ preventScroll: true });
          } catch (_err) {}
          try {
            ctrl.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true, cancelable: true }));
            ctrl.dispatchEvent(new MouseEvent('pointerup', { bubbles: true, cancelable: true }));
            ctrl.dispatchEvent(new MouseEvent('mouseenter', { bubbles: true, cancelable: true }));
            ctrl.dispatchEvent(new MouseEvent('mouseover', { bubbles: true, cancelable: true }));
            ctrl.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, cancelable: true }));
            ctrl.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
            if (typeof ctrl.click === 'function') ctrl.click();
          } catch (_err) {}
          await sleep(240);
          const overlays = [];
          try {
            document
              .querySelectorAll('[role="tooltip"],[role="dialog"],[data-slot="content"],[class*="popover"],[class*="tooltip"]')
              .forEach((node) => {
                if (isElementVisible(node) && !overlays.includes(node)) overlays.push(node);
              });
          } catch (_err) {}
          let scopedUrls = [];
          overlays.forEach((node) => {
            scopedUrls = scopedUrls.concat(extractUrlsFromElement(node));
          });
          const after = collectVisibleDocumentMediaUrls();
          const diffUrls = after.filter((url) => !before.has(url));
          const taskUrls = networkMediaCapture.byTask(record.taskId);
          const networkDiff = networkMediaCapture.diff(beforeCaptured, record.taskId);
          const docTempfileUrls = collectTempfileUrlsFromDocument();
          const candidates = sortMediaUrls([
            ...taskUrls,
            ...networkDiff,
            ...docTempfileUrls,
            ...scopedUrls,
            ...diffUrls,
            ...after.filter((url) => /(tempfile\\.aiquickdraw|result|output|download|generated)/i.test(url))
          ]);
          const tempfileCandidates = candidates.filter((url) => isTempfileUrl(url));
          const preferred = tempfileCandidates.length
            ? tempfileCandidates
            : candidates.filter((url) => !baseRowUrls.has(url));
          const chosen = preferred.length ? preferred : candidates;
          const mergedResult = new Set(Array.isArray(record.resultUrls) ? record.resultUrls : []);
          chosen.forEach((url) => mergedResult.add(url));
          const sortedResult = sortMediaUrls(Array.from(mergedResult));
          if (sortedResult.length) {
            record.resultUrls = sortedResult;
            record.urls = sortedResult;
          } else {
            record.urls = sortMediaUrls(Array.isArray(record.urls) ? record.urls : []);
          }
          recordMap.set(key, record);
          try {
            document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
          } catch (_err) {}
          await sleep(40);
          if (Array.isArray(record.resultUrls) && record.resultUrls.some((url) => isTempfileUrl(url))) break;
        }
      }
    };

    const getScrollableContainer = (rows = []) => {
      const allCandidates = [];
      const pushCandidate = (el) => {
        if (!el || allCandidates.includes(el)) return;
        allCandidates.push(el);
      };

      pushCandidate(document.scrollingElement);
      pushCandidate(document.documentElement);
      pushCandidate(document.body);

      try {
        document.querySelectorAll('*').forEach((el) => {
          const style = getComputedStyle(el);
          const overflowY = String(style.overflowY || style.overflow || '').toLowerCase();
          const scrollable = overflowY.includes('auto') || overflowY.includes('scroll');
          if (scrollable && el.scrollHeight > el.clientHeight + 120) {
            pushCandidate(el);
          }
        });
      } catch (_err) {}

      let best = document.scrollingElement || document.documentElement || document.body;
      let bestScore = -1;
      allCandidates.forEach((el) => {
        const containsRows = rows.reduce((count, row) => (el && row && el.contains(row) ? count + 1 : count), 0);
        const heightScore = Math.max(0, Number(el.scrollHeight || 0) - Number(el.clientHeight || 0));
        const score = containsRows * 1000 + heightScore;
        if (score > bestScore) {
          bestScore = score;
          best = el;
        }
      });
      return best;
    };

    const getScrollTop = (el) => {
      if (!el) return 0;
      const isPage = el === document.body || el === document.documentElement || el === document.scrollingElement;
      if (isPage) {
        return Number(window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0);
      }
      return Number(el.scrollTop || 0);
    };

    const getScrollMax = (el) => {
      if (!el) return 0;
      const isPage = el === document.body || el === document.documentElement || el === document.scrollingElement;
      if (isPage) {
        const full = Math.max(
          Number(document.body?.scrollHeight || 0),
          Number(document.documentElement?.scrollHeight || 0)
        );
        return Math.max(0, full - Number(window.innerHeight || 0));
      }
      return Math.max(0, Number(el.scrollHeight || 0) - Number(el.clientHeight || 0));
    };

    const setScrollTop = (el, value) => {
      if (!el) return;
      const next = Math.max(0, Number(value || 0));
      const isPage = el === document.body || el === document.documentElement || el === document.scrollingElement;
      if (isPage) {
        window.scrollTo(0, next);
      } else {
        el.scrollTop = next;
      }
    };

    const parsePositiveInt = (value) => {
      const text = toText(value).replace(/,/g, '');
      if (!text) return 0;
      if (!/^\d{1,6}$/.test(text)) return 0;
      const parsed = Number.parseInt(text, 10);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
    };

    const readPageNumberFromNode = (el) => {
      if (!el) return 0;
      const attrHints = [
        el.getAttribute ? el.getAttribute('data-page') : '',
        el.getAttribute ? el.getAttribute('data-page-number') : '',
        el.getAttribute ? el.getAttribute('aria-label') : '',
        el.getAttribute ? el.getAttribute('title') : '',
        el.textContent || ''
      ];
      for (const hint of attrHints) {
        const text = toText(hint);
        if (!text) continue;
        const direct = parsePositiveInt(text);
        if (direct > 0) return direct;
        const pageMatch = text.match(/(?:page|p)\\s*[:#]?\\s*(\\d{1,6})/i);
        if (pageMatch && pageMatch[1]) {
          const parsed = Number.parseInt(pageMatch[1], 10);
          if (Number.isFinite(parsed) && parsed > 0) return parsed;
        }
      }
      return 0;
    };

    const getPaginationContainers = () => {
      const candidates = new Set();
      const selectors = [
        '[class*="MuiTablePagination-root"]',
        '[class*="MuiPagination-root"]',
        '[class*="pagination"]',
        '[class*="Pagination"]',
        '[aria-label*="pagination" i]',
        'nav[data-slot="base"][role="navigation"][data-total][data-active-page]',
        '[data-slot="base"][data-total][data-active-page]'
      ];
      selectors.forEach((selector) => {
        try {
          document.querySelectorAll(selector).forEach((el) => {
            if (!isElementVisible(el)) return;
            candidates.add(el);
          });
        } catch (_err) {}
      });
      try {
        document.querySelectorAll('div,nav,footer,section').forEach((el) => {
          if (!isElementVisible(el)) return;
          const text = toText(el.innerText || el.textContent || '');
          if (!text || text.length > 500) return;
          if (/(rows\\s*per\\s*page|go\\s*to\\s*\\d*\\s*page|go\\s*to\\s*page|\\u6bcf\\s*\\u9875|\\u8df3\\s*\\u8f6c)/i.test(text)) {
            candidates.add(el);
          }
        });
      } catch (_err) {}

      const scoreContainer = (el) => {
        if (!el) return -999;
        const text = toText(el.innerText || el.textContent || '');
        if (!text) return -999;
        const clickableCount = (() => {
          try {
            return el.querySelectorAll('button,a,[role="button"],li').length;
          } catch (_err) {
            return 0;
          }
        })();
        if (clickableCount < 3) return -999;
        let score = 0;
        if (/(rows\\s*per\\s*page|go\\s*to|\\u6bcf\\s*\\u9875|\\u8df3\\s*\\u8f6c)/i.test(text)) score += 60;
        if (/(next|prev|previous|>|<|»|«|\\u4e0a\\u4e00\\u9875|\\u4e0b\\u4e00\\u9875)/i.test(text)) score += 30;
        const pageNums = (text.match(/\\b\\d{1,4}\\b/g) || []).length;
        score += Math.min(36, pageNums * 3);
        if (/\\d{4}[\\/\\-]\\d{1,2}[\\/\\-]\\d{1,2}/.test(text)) score -= 55;
        if (text.length > 700) score -= 40;
        if (text.length > 420) score -= 16;
        if (el.querySelector?.('tbody tr,[role="row"],table')) score -= 45;
        score += Math.min(18, clickableCount);
        return score;
      };

      const ranked = Array.from(candidates)
        .map((el) => ({ el, score: scoreContainer(el) }))
        .filter((entry) => entry.score > -200)
        .sort((a, b) => b.score - a.score);
      if (!ranked.length) return [];

      const expand = new Set();
      ranked.slice(0, 3).forEach((entry) => {
        const base = entry.el;
        if (!base) return;
        expand.add(base);
        if (base.parentElement) expand.add(base.parentElement);
        if (base.parentElement?.parentElement) expand.add(base.parentElement.parentElement);
        try {
          const localExtra = base.querySelectorAll(
            '[class*="MuiPagination-root"],[class*="MuiTablePagination-actions"],[class*="pagination"],nav,ul'
          );
          localExtra.forEach((el) => expand.add(el));
        } catch (_err) {}
        try {
          const parentExtra = base.parentElement?.querySelectorAll?.(
            '[class*="MuiPagination-root"],[class*="MuiTablePagination-actions"],[class*="pagination"],nav,ul'
          );
          parentExtra?.forEach((el) => expand.add(el));
        } catch (_err) {}
      });

      return Array.from(expand).filter((el) => isElementVisible(el));
    };

    const getPaginationGoToInput = (containers = []) => {
      const scorePaginationInput = (input, container = null) => {
        if (!input || !isElementVisible(input)) return -1;
        const attrText = toText(
          String(input.getAttribute?.('aria-label') || '') +
            ' ' +
            String(input.getAttribute?.('placeholder') || '') +
            ' ' +
            String(input.getAttribute?.('name') || '') +
            ' ' +
            String(input.getAttribute?.('title') || '')
        ).toLowerCase();
        const contextText = toText(input.parentElement?.innerText || '').toLowerCase();
        const inputType = String(input.getAttribute?.('type') || input.type || '').toLowerCase();
        const inputSlot = toText(String(input.getAttribute?.('data-slot') || '')).toLowerCase();
        const minValue = parsePositiveInt(input.getAttribute?.('min') || input.min || '');
        const maxValue = parsePositiveInt(input.getAttribute?.('max') || input.max || '');
        const currentValue = parsePositiveInt(input.value || input.getAttribute?.('value') || '');
        const closestNav = input.closest
          ? input.closest('nav,[role="navigation"],[data-slot="base"],[aria-label*="pagination" i]')
          : null;
        const navText = toText(
          String(closestNav?.getAttribute?.('aria-label') || '') +
            ' ' +
            String(closestNav?.getAttribute?.('data-slot') || '') +
            ' ' +
            String(closestNav?.getAttribute?.('role') || '')
        ).toLowerCase();
        const hasPageHint =
          /(go\\s*to|page|\\u9875|\\u8df3\\s*\\u8f6c|pagination|pager)/i.test(attrText) ||
          /(go\\s*to|page|\\u9875|\\u8df3\\s*\\u8f6c|pagination|pager)/i.test(contextText);
        let score = 0;
        if (hasPageHint) score += 80;
        if (inputType === 'number') score += 26;
        if (inputSlot === 'input') score += 40;
        if (minValue >= 1) score += 12;
        if (maxValue >= 2) score += 24;
        if (currentValue >= 1 && maxValue > 0 && currentValue <= maxValue) score += 6;
        if (closestNav) score += 22;
        if (/(pagination|navigation)/i.test(navText)) score += 24;
        if (parsePositiveInt(closestNav?.getAttribute?.('data-total') || '') >= 2) score += 34;
        if (container && container.contains?.(input)) score += 12;
        return score;
      };
      let bestInput = null;
      let bestScore = -1;
      for (const container of containers) {
        let inputs = [];
        try {
          inputs = Array.from(
            container.querySelectorAll(
              'input[type="number"], input[data-slot="input"], input[aria-label*="page" i], input[aria-label*="go" i], input'
            )
          );
        } catch (_err) {
          inputs = [];
        }
        for (const input of inputs) {
          const score = scorePaginationInput(input, container);
          if (score > bestScore) {
            bestScore = score;
            bestInput = input;
          }
        }
      }
      if (bestInput && bestScore >= 46) return bestInput;
      return null;
    };

    const getGlobalPaginationGoToInput = () => {
      try {
        const allInputs = Array.from(
          document.querySelectorAll(
            'input[type="number"], input[data-slot="input"], input[aria-label*="page" i], input[aria-label*="go" i], input'
          )
        );
        let bestInput = null;
        let bestScore = -1;
        for (const input of allInputs) {
          if (!input || !isElementVisible(input)) continue;
          const attrText = toText(
            String(input.getAttribute?.('aria-label') || '') +
              ' ' +
              String(input.getAttribute?.('placeholder') || '') +
              ' ' +
              String(input.getAttribute?.('name') || '') +
              ' ' +
              String(input.getAttribute?.('title') || '') +
              ' ' +
              String(input.getAttribute?.('data-slot') || '')
          ).toLowerCase();
          const contextText = toText(input.parentElement?.innerText || '').toLowerCase();
          const isNumberType = String(input.getAttribute?.('type') || input.type || '').toLowerCase() === 'number';
          const minValue = parsePositiveInt(input.getAttribute?.('min') || input.min || '');
          const maxValue = parsePositiveInt(input.getAttribute?.('max') || input.max || '');
          const closestNav = input.closest
            ? input.closest('nav,[role="navigation"],[data-slot="base"],[aria-label*="pagination" i]')
            : null;
          let score = 0;
          if (/(go\\s*to|page|\\u9875|\\u8df3\\s*\\u8f6c|pagination|pager)/i.test(attrText)) score += 90;
          if (/(go\\s*to|page|\\u9875|\\u8df3\\s*\\u8f6c|pagination|pager)/i.test(contextText)) score += 55;
          if (isNumberType) score += 24;
          if (String(input.getAttribute?.('data-slot') || '').toLowerCase() === 'input') score += 38;
          if (minValue >= 1) score += 10;
          if (maxValue >= 2) score += 24;
          if (closestNav) score += 20;
          if (parsePositiveInt(closestNav?.getAttribute?.('data-total') || '') >= 2) score += 32;
          if (score > bestScore) {
            bestScore = score;
            bestInput = input;
          }
        }
        if (bestInput && bestScore >= 46) return bestInput;
      } catch (_err) {}
      return null;
    };

    const getPaginationControls = (containers = []) => {
      const controls = [];
      const seen = new Set();
      const isControlDisabled = (target, parent = null) => {
        if (target?.hasAttribute?.('disabled') || parent?.hasAttribute?.('disabled')) return true;
        const ariaDisabled = String(
          target?.getAttribute?.('aria-disabled') || parent?.getAttribute?.('aria-disabled') || ''
        )
          .trim()
          .toLowerCase();
        if (ariaDisabled === 'true' || ariaDisabled === '1') return true;
        const dataDisabled = String(
          target?.getAttribute?.('data-disabled') || parent?.getAttribute?.('data-disabled') || ''
        )
          .trim()
          .toLowerCase();
        if (dataDisabled === 'true' || dataDisabled === '1') return true;
        const classBlob = (
          String(target?.className || '') + ' ' + String(parent?.className || '')
        ).toLowerCase();
        if (/data-\[disabled=true\]/.test(classBlob)) return false;
        if (/(^|\s)(is-disabled|disabled)(\s|$)/.test(classBlob)) return true;
        return false;
      };
      const normalizeKind = (label = '', pageNumber = 0) => {
        const text = String(label || '').toLowerCase();
        if (pageNumber > 0) return 'page';
        if (
          /(^|\\b)(next|>|»|\\u4e0b\\u4e00\\u9875|navigate_next|keyboard_arrow_right|chevron_right|arrow_forward|previousnext)(\\b|$)/i.test(
            text
          )
        ) {
          return 'next';
        }
        if (
          /(^|\\b)(prev|previous|<|«|\\u4e0a\\u4e00\\u9875|navigate_before|keyboard_arrow_left|chevron_left|arrow_back|previousnext)(\\b|$)/i.test(
            text
          )
        ) {
          return 'prev';
        }
        if (/(^|\\b)(first|\\u9996\\u9875)(\\b|$)/i.test(text)) return 'first';
        if (/(^|\\b)(last|\\u5c3e\\u9875)(\\b|$)/i.test(text)) return 'last';
        return 'other';
      };
      containers.forEach((container) => {
        let nodes = [];
        try {
          nodes = Array.from(
            container.querySelectorAll(
              'button,a,[role="button"],li,[aria-label],[title],[data-page],[data-page-number]'
            )
          );
        } catch (_err) {
          nodes = [];
        }
        nodes.forEach((node) => {
          if (!node) return;
          let clickable = node;
          if (!/^(button|a)$/i.test(String(node.tagName || ''))) {
            const nested = node.querySelector ? node.querySelector('button,a,[role="button"]') : null;
            if (nested) clickable = nested;
          }
          if (!clickable || seen.has(clickable)) return;
          if (!isElementVisible(clickable)) return;
          const tagName = String(clickable.tagName || '').toLowerCase();
          const tagIsDirectControl = /^(button|a|li)$/.test(tagName);
          const label = toText(
            (clickable.getAttribute && (clickable.getAttribute('aria-label') || clickable.getAttribute('title'))) ||
              clickable.innerText ||
              clickable.textContent ||
              ''
          );
          const classHints = toText(
            String(clickable.className || '') +
              ' ' +
              String(node.className || '') +
              ' ' +
              String(clickable.getAttribute?.('data-slot') || '') +
              ' ' +
              String(node.getAttribute?.('data-slot') || '')
          ).toLowerCase();
          const paginationHints = toText(label + ' ' + classHints);
          if (
            !tagIsDirectControl &&
            !/(pagination|pager|page|rows\\s*per\\s*page|go\\s*to|navigate|chevron|arrow)/i.test(paginationHints)
          ) {
            return;
          }
          const pageNumber = readPageNumberFromNode(clickable) || readPageNumberFromNode(node);
          const kind = normalizeKind((label + ' ' + classHints).trim(), pageNumber);
          if (kind === 'other' && !pageNumber) return;
          const disabled = isControlDisabled(clickable, node);
          const active =
            clickable.getAttribute?.('aria-current') === 'page' ||
            clickable.getAttribute?.('aria-current') === 'true' ||
            clickable.getAttribute?.('data-active') === 'true' ||
            node.getAttribute?.('aria-current') === 'page' ||
            node.getAttribute?.('aria-current') === 'true' ||
            node.getAttribute?.('data-active') === 'true' ||
            /\\bactive\\b/.test(String(clickable.getAttribute?.('aria-label') || '').toLowerCase()) ||
            /\\bactive\\b/.test(String(node.getAttribute?.('aria-label') || '').toLowerCase()) ||
            /active|current|selected/.test(String(clickable.className || '').toLowerCase()) ||
            /active|current|selected/.test(String(node.className || '').toLowerCase());
          seen.add(clickable);
          controls.push({
            el: clickable,
            pageNumber,
            kind,
            label: label.toLowerCase(),
            slot: toText(
              String(clickable.getAttribute?.('data-slot') || '') + ' ' + String(node.getAttribute?.('data-slot') || '')
            ).toLowerCase(),
            disabled,
            active
          });
        });
      });
      return controls;
    };

    const getCurrentPageFromPagination = (containers = getPaginationContainers()) => {
      const activePageValues = [];
      for (const container of containers) {
        const fromActivePage = parsePositiveInt(container?.getAttribute?.('data-active-page') || '');
        if (fromActivePage > 0) activePageValues.push(fromActivePage);
      }
      if (activePageValues.length) {
        const sorted = activePageValues
          .filter((value) => Number.isFinite(value) && value > 0 && value <= 5000)
          .sort((a, b) => b - a);
        if (sorted.length) return sorted[0];
      }
      const controls = getPaginationControls(containers);
      const activePage = controls.find((control) => control.active && control.pageNumber > 0);
      if (activePage) return activePage.pageNumber;
      const goToInput = getPaginationGoToInput(containers);
      const inputPage = parsePositiveInt(goToInput?.value || goToInput?.getAttribute?.('value') || '');
      if (inputPage > 0) return inputPage;
      const allPages = controls
        .map((control) => Number(control.pageNumber || 0))
        .filter((value) => Number.isFinite(value) && value > 0)
        .sort((a, b) => a - b);
      const plausible = allPages.filter((value) => value > 0 && value <= 5000);
      if (plausible.length) {
        const max = plausible[plausible.length - 1];
        if (max > 1) return max;
        if (plausible.includes(1)) return 1;
        if (plausible.length <= 8) {
          const min = plausible[0];
          if (max - min <= 20) return min;
        }
      }
      return 1;
    };

    const getHasNextPageFromPagination = (containers = getPaginationContainers()) => {
      const controls = getPaginationControls(containers);
      const nextCtrl = controls.find((control) => control.kind === 'next');
      if (nextCtrl) return !nextCtrl.disabled;
      const current = getCurrentPageFromPagination(containers);
      const maxByDataTotal = containers.reduce((acc, container) => {
        const value = parsePositiveInt(container?.getAttribute?.('data-total') || '');
        if (!Number.isFinite(value) || value <= 0 || value > 5000) return acc;
        return Math.max(acc, value);
      }, 0);
      if (maxByDataTotal > 0) {
        return current < maxByDataTotal;
      }
      const goToInput = getPaginationGoToInput(containers) || getGlobalPaginationGoToInput();
      const maxByInput = parsePositiveInt(goToInput?.getAttribute?.('max') || goToInput?.max || '');
      if (maxByInput > 0) {
        return current < maxByInput;
      }
      const maxPage = controls.reduce((acc, control) => {
        const value = Number(control.pageNumber || 0);
        if (!Number.isFinite(value) || value <= 0 || value > 5000) return acc;
        return Math.max(acc, value);
      }, 0);
      return maxPage > current;
    };

    const getTotalPagesFromPagination = (containers = getPaginationContainers()) => {
      const fromDataTotal = containers.reduce((acc, container) => {
        const value = parsePositiveInt(container?.getAttribute?.('data-total') || '');
        if (!Number.isFinite(value) || value <= 0 || value > 5000) return acc;
        return Math.max(acc, value);
      }, 0);
      if (fromDataTotal > 0) return fromDataTotal;
      const goToInput = getPaginationGoToInput(containers) || getGlobalPaginationGoToInput();
      const fromInputMax = parsePositiveInt(goToInput?.getAttribute?.('max') || goToInput?.max || '');
      if (fromInputMax > 0) return fromInputMax;
      const controls = getPaginationControls(containers);
      let total = 0;
      controls.forEach((control) => {
        const value = Number(control.pageNumber || 0);
        if (Number.isFinite(value) && value > 0 && value <= 5000 && value > total) total = value;
      });
      const current = getCurrentPageFromPagination(containers);
      const hasNext = getHasNextPageFromPagination(containers);
      if (hasNext && total <= current) total = current + 1;
      if (!total) total = current;
      return Math.max(1, total);
    };

    const clickLikeUser = (el) => {
      if (!el) return false;
      try {
        if (typeof el.scrollIntoView === 'function') {
          el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
        }
      } catch (_err) {}
      try {
        el.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true, cancelable: true }));
        el.dispatchEvent(new MouseEvent('pointerup', { bubbles: true, cancelable: true }));
        el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
      } catch (_err) {}
      try {
        if (typeof el.click === 'function') {
          el.click();
          return true;
        }
      } catch (_err) {}
      return true;
    };

    const getRowsSignature = () => {
      const rows = getRowCandidates();
      const head = rows
        .slice(0, 6)
        .map((row) => hashText(toText(row.innerText || row.textContent || '').slice(0, 220)));
      return rows.length + '|' + head.join('|');
    };

    const waitForRowsSettle = async ({ expectedPage = 0, beforeSignature = '', timeoutMs = 8000 } = {}) => {
      const startedAt = Date.now();
      let stableCount = 0;
      let lastSignature = '';
      let pollCount = 0;
      pushDebug('wait:settle-start', {
        expectedPage,
        timeoutMs,
        hasBeforeSignature: !!beforeSignature
      });
      while (Date.now() - startedAt < timeoutMs) {
        pollCount += 1;
        const nowSignature = getRowsSignature();
        const rows = getRowCandidates();
        const currentPage = getCurrentPageFromPagination();
        const changed = !beforeSignature || nowSignature !== beforeSignature;
        const pageMatched = expectedPage <= 0 || currentPage === expectedPage;
        if (pageMatched && changed && rows.length) {
          if (nowSignature === lastSignature) stableCount += 1;
          else stableCount = 0;
          lastSignature = nowSignature;
          if (stableCount >= 2) {
            pushDebug('wait:settle-ready', {
              expectedPage,
              pollCount,
              elapsedMs: Date.now() - startedAt,
              rowCount: rows.length,
              currentPage
            });
            return true;
          }
        }
        if (pollCount === 1 || pollCount % 8 === 0) {
          pushDebug('wait:settle-poll', {
            expectedPage,
            pollCount,
            elapsedMs: Date.now() - startedAt,
            rowCount: rows.length,
            currentPage,
            pageMatched,
            changed,
            stableCount
          });
        }
        await sleep(Math.max(110, Math.min(260, waitMs)));
      }
      const rows = getRowCandidates();
      const lastSignatureNow = getRowsSignature();
      const finalPage = getCurrentPageFromPagination();
      const pageMatched = expectedPage <= 0 || finalPage === expectedPage;
      const signatureChanged = !beforeSignature || lastSignatureNow !== beforeSignature;
      pushDebug('wait:settle-timeout', {
        expectedPage,
        timeoutMs,
        rowCount: rows.length,
        currentPage: finalPage,
        pageMatched,
        signatureChanged
      });
      if (!rows.length) return false;
      if (expectedPage > 1 && !pageMatched && signatureChanged) {
        pushDebug('wait:settle-accept-signature', {
          expectedPage,
          timeoutMs,
          rowCount: rows.length,
          currentPage: finalPage
        });
        return true;
      }
      if (!pageMatched) return false;
      if (beforeSignature && !signatureChanged) return false;
      return true;
    };

    const navigateToPage = async (targetPage = 1, preferredContainers = []) => {
      const desired = Math.max(1, Number(targetPage) || 1);
      const containers = Array.isArray(preferredContainers) && preferredContainers.length
        ? preferredContainers
        : getPaginationContainers();
      pushDebug('navigate:init', { desired, containerCount: containers.length });
      if (!containers.length) {
        pushDebug('navigate:no-pagination', { desired });
        return false;
      }

      const goToInput = getPaginationGoToInput(containers);
      if (goToInput || getGlobalPaginationGoToInput()) {
        const directInput = goToInput || getGlobalPaginationGoToInput();
        const setNativeInputValue = (input, value) => {
          if (!input) return;
          const nextValue = String(value ?? '');
          try {
            const proto = Object.getPrototypeOf(input);
            const desc = proto ? Object.getOwnPropertyDescriptor(proto, 'value') : null;
            if (desc && typeof desc.set === 'function') {
              desc.set.call(input, nextValue);
              return;
            }
          } catch (_err) {}
          try {
            input.value = nextValue;
          } catch (_err) {}
        };
        const beforeSignature = getRowsSignature();
        try {
          directInput.focus?.();
          setNativeInputValue(directInput, desired);
          directInput.dispatchEvent(new Event('input', { bubbles: true }));
          directInput.dispatchEvent(new Event('change', { bubbles: true }));
          directInput.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true }));
          directInput.dispatchEvent(new KeyboardEvent('keypress', { key: 'Enter', bubbles: true, cancelable: true }));
          directInput.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true, cancelable: true }));
          directInput.dispatchEvent(new FocusEvent('blur', { bubbles: true }));
          pushDebug('navigate:goto-submit', { desired });
          const settled = await waitForRowsSettle({ expectedPage: desired, beforeSignature, timeoutMs: 5200 });
          const afterGoToPage = getCurrentPageFromPagination(containers);
          pushDebug('navigate:goto-result', { desired, afterGoToPage, settled });
          if (afterGoToPage === desired) return true;
          if (settled && desired > 1) {
            pushDebug('navigate:goto-accept-settle', { desired, afterGoToPage });
            return true;
          }
        } catch (err) {
          pushDebug('navigate:goto-error', { desired, message: String(err?.message || 'error') });
        }
      }

      let guard = 0;
      while (guard < 12) {
        guard += 1;
        const current = getCurrentPageFromPagination(containers);
        pushDebug('navigate:step', { desired, current, guard });
        if (current === desired) return true;

        const controls = getPaginationControls(containers);
        const exact = controls.find((control) => control.kind === 'page' && control.pageNumber === desired && !control.disabled);
        const beforeSignature = getRowsSignature();
        if (exact?.el) {
          pushDebug('navigate:click-exact', { desired, current, guard });
          clickLikeUser(exact.el);
          await waitForRowsSettle({ expectedPage: desired, beforeSignature, timeoutMs: 4600 });
          if (getCurrentPageFromPagination(containers) === desired) return true;
          pushDebug('navigate:exact-not-settled', { desired, guard });
          continue;
        }

        const direction = desired > current ? 'next' : 'prev';
        const stepControl = controls.find((control) => control.kind === direction && !control.disabled);
        if (!stepControl?.el) {
          const refreshedContainers = getPaginationContainers();
          const refreshedControls = getPaginationControls(refreshedContainers);
          const findGlobalFallbackControl = (wantedKind, wantedPage = 0) => {
            let nodes = [];
            try {
              nodes = Array.from(
                document.querySelectorAll(
                  'button,a,[role="button"],li,[aria-label],[title],[data-page],[data-page-number]'
                )
              );
            } catch (_err) {
              nodes = [];
            }
            const isDisabled = (el) => {
              if (el?.hasAttribute?.('disabled')) return true;
              const ariaDisabled = String(el?.getAttribute?.('aria-disabled') || '')
                .trim()
                .toLowerCase();
              if (ariaDisabled === 'true' || ariaDisabled === '1') return true;
              const dataDisabled = String(el?.getAttribute?.('data-disabled') || '')
                .trim()
                .toLowerCase();
              if (dataDisabled === 'true' || dataDisabled === '1') return true;
              const classBlob = String(el?.className || '').toLowerCase();
              if (/data-\[disabled=true\]/.test(classBlob)) return false;
              if (/(^|\s)(is-disabled|disabled)(\s|$)/.test(classBlob)) return true;
              return false;
            };
            const matchDirection = (text, kind) => {
              const value = String(text || '').toLowerCase();
              if (kind === 'next') {
                return /(^|\\b)(next|>|»|\\u4e0b\\u4e00\\u9875|navigate_next|keyboard_arrow_right|chevron_right|arrow_forward|previousnext)(\\b|$)/i.test(
                  value
                );
              }
              return /(^|\\b)(prev|previous|<|«|\\u4e0a\\u4e00\\u9875|navigate_before|keyboard_arrow_left|chevron_left|arrow_back|previousnext)(\\b|$)/i.test(
                value
              );
            };
            for (const node of nodes) {
              if (!node || !isElementVisible(node) || isDisabled(node)) continue;
              const pageNumber = readPageNumberFromNode(node);
              if (wantedKind === 'page' && pageNumber === wantedPage) return node;
              const label = toText(
                String(node.getAttribute?.('aria-label') || '') +
                  ' ' +
                  String(node.getAttribute?.('title') || '') +
                  ' ' +
                  String(node.innerText || node.textContent || '') +
                  ' ' +
                  String(node.className || '')
              );
              if (wantedKind !== 'page' && matchDirection(label, wantedKind)) return node;
            }
            return null;
          };
          const refreshedStep = refreshedControls.find((control) => control.kind === direction && !control.disabled);
          if (refreshedStep?.el) {
            pushDebug('navigate:click-step-refreshed', { desired, current, direction, guard });
            clickLikeUser(refreshedStep.el);
            const expectedStepPage = Math.max(1, current + (direction === 'next' ? 1 : -1));
            await waitForRowsSettle({ expectedPage: expectedStepPage, beforeSignature, timeoutMs: 4200 });
            continue;
          }
          const globalExact = findGlobalFallbackControl('page', desired);
          if (globalExact) {
            pushDebug('navigate:click-global-exact', { desired, current, guard });
            clickLikeUser(globalExact);
            await waitForRowsSettle({ expectedPage: desired, beforeSignature, timeoutMs: 4600 });
            if (getCurrentPageFromPagination(getPaginationContainers()) === desired) return true;
          }
          const globalStep = findGlobalFallbackControl(direction);
          if (globalStep) {
            pushDebug('navigate:click-global-step', { desired, current, direction, guard });
            clickLikeUser(globalStep);
            const expectedStepPage = Math.max(1, current + (direction === 'next' ? 1 : -1));
            await waitForRowsSettle({ expectedPage: expectedStepPage, beforeSignature, timeoutMs: 4200 });
            continue;
          }
          const controlSample = controls.slice(0, 12).map((control) => ({
            kind: control.kind,
            pageNumber: Number(control.pageNumber || 0),
            slot: String(control.slot || ''),
            disabled: !!control.disabled,
            active: !!control.active,
            label: String(control.label || '').slice(0, 48)
          }));
          const refreshedSample = refreshedControls.slice(0, 12).map((control) => ({
            kind: control.kind,
            pageNumber: Number(control.pageNumber || 0),
            slot: String(control.slot || ''),
            disabled: !!control.disabled,
            active: !!control.active,
            label: String(control.label || '').slice(0, 48)
          }));
          pushDebug('navigate:no-step-control', { desired, current, direction, guard });
          pushDebug('navigate:no-step-sample', {
            desired,
            current,
            direction,
            guard,
            controls: controlSample,
            refreshed: refreshedSample
          });
          break;
        }
        pushDebug('navigate:click-step', { desired, current, direction, guard });
        clickLikeUser(stepControl.el);
        const expectedStepPage = Math.max(1, current + (direction === 'next' ? 1 : -1));
        const settled = await waitForRowsSettle({ expectedPage: expectedStepPage, beforeSignature, timeoutMs: 4200 });
        if (settled && expectedStepPage === desired) {
          const pageAfterStep = getCurrentPageFromPagination(getPaginationContainers());
          pushDebug('navigate:step-result', {
            desired,
            current,
            expectedStepPage,
            pageAfterStep,
            guard
          });
          if (pageAfterStep === desired || desired > 1) return true;
        }
      }
      const reached = getCurrentPageFromPagination(containers) === desired;
      pushDebug('navigate:done', { desired, reached, guard });
      return reached;
    };

    const looksLoggedOut = () => {
      const text = String(document.body?.innerText || '').slice(0, 12000).toLowerCase();
      const hasLoginText = /sign\\s*in|log\\s*in|continue\\s*with\\s*google|get\\s*started/.test(text);
      const hasLogsText = /task\\s*id|recent\\s*history|logs/.test(text);
      return hasLoginText && !hasLogsText;
    };

    const harvest = (recordMap) => {
      const rows = getRowCandidates();
      rows.forEach((row, index) => {
        const text = toText(row.innerText || row.textContent || '');
        if (!text) return;
        if (/^time\\s+type\\s+param\\s+status/i.test(text.toLowerCase())) return;
        const taskId = extractTaskId(text, row);
        const urls = extractUrlsFromElement(row);
        if (!taskId && !urls.length && !/\\d{4}[\\/\\-]\\d{1,2}[\\/\\-]\\d{1,2}/.test(text)) return;
        const prompt = extractPrompt(row, text);
        const state = extractStatus(text);
        const stamp = extractTimeMs(text);
        const rawKey =
          taskId || row.getAttribute('data-key') || row.getAttribute('data-id') || hashText(text + '|' + urls.join('|'));
        if (!rawKey) return;
        const signature = hashText(
          String(taskId || '') +
            '|' +
            String(text || '').slice(0, 240) +
            '|' +
            String(stamp || 0) +
            '|' +
            String(urls.slice(0, 4).join('|') || '')
        );
        let key = String(rawKey);
        if (recordMap.has(key)) {
          const existing = recordMap.get(key);
          const existingSig = String(existing?._sig || '');
          if (existingSig && existingSig !== signature) {
            key = String(key) + '-' + String(index) + '-' + String(signature).slice(0, 8);
          }
        }

        const prev = recordMap.get(key);
        const next = prev
          ? { ...prev }
          : {
              id: String(key),
              taskId: taskId || '',
              state: state || 'unknown',
              model: '',
              createTime: stamp || 0,
              updateTime: stamp || 0,
              completeTime: 0,
              failMsg: '',
              failCode: '',
              prompt: '',
              urls: [],
              resultUrls: [],
              _order: Number(recordMap.size || index || 0),
              _sig: signature
            };

        if (taskId && !next.taskId) next.taskId = taskId;
        if (state && next.state === 'unknown') next.state = state;
        if (stamp > 0) {
          next.updateTime = Math.max(Number(next.updateTime || 0), stamp);
          next.createTime = Number(next.createTime || 0) || stamp;
        }
        if (prompt && (!next.prompt || prompt.length > next.prompt.length)) {
          next.prompt = prompt;
        }
        const merged = new Set([...(Array.isArray(next.urls) ? next.urls : []), ...urls]);
        next.urls = sortMediaUrls(Array.from(merged));
        next._sig = signature;
        recordMap.set(key, next);
      });
      return rows;
    };

    return (async () => {
      try {
        if (looksLoggedOut()) {
          pushDebug('auth:logged-out');
          return {
            ok: false,
            authError: true,
            error: 'Not signed in to kie.ai logs yet. Sign in first.',
            debug
          };
        }

        const records = new Map();
        const paginationContainers = getPaginationContainers();
        const hasPagination = paginationContainers.length > 0;
        pushDebug('pagination:detected', { hasPagination, containerCount: paginationContainers.length });

        if (hasPagination) {
          const desiredPage = Math.max(1, Number(pageNum) || 1);
          const currentPage = getCurrentPageFromPagination(paginationContainers);
          pushDebug('pagination:state', { desiredPage, currentPage });
          if (desiredPage > 1 && currentPage !== desiredPage) {
            const moved = await navigateToPage(desiredPage, paginationContainers);
            if (!moved) {
              pushDebug('pagination:navigate-failed', {
                desiredPage,
                currentPage: getCurrentPageFromPagination(paginationContainers)
              });
              return {
                ok: false,
                error: 'Could not open the requested logs page.',
                debug
              };
            }
          } else if (desiredPage === 1 && currentPage !== 1) {
            pushDebug('pagination:page1-no-nav', { desiredPage, currentPage });
          } else {
            await waitForRowsSettle({ expectedPage: desiredPage, beforeSignature: '', timeoutMs: 7000 });
          }

          let stable = 0;
          let lastSignature = '';
          const harvestMaxSteps = fastRowsOnly
            ? Math.max(6, Math.min(14, maxScrollSteps))
            : Math.max(8, Math.min(26, maxScrollSteps));
          pushDebug('pagination:harvest-start', {
            desiredPage,
            maxSteps: harvestMaxSteps
          });
          for (let step = 0; step < harvestMaxSteps; step += 1) {
            harvest(records);
            const signature = getRowsSignature();
            if (signature && signature === lastSignature && records.size > 0) {
              stable += 1;
            } else {
              stable = 0;
            }
            lastSignature = signature;
          if (step === 0 || step % 6 === 0) {
            pushDebug('pagination:harvest-poll', {
              desiredPage,
              step,
              stable,
              recordCount: records.size,
              rowCount: getRowCandidates().length,
              currentPage: getCurrentPageFromPagination()
            });
          }
            if (stable >= 2) {
              pushDebug('pagination:harvest-ready', {
                desiredPage,
                step,
                stable,
                recordCount: records.size
              });
              break;
            }
            await sleep(Math.max(110, Math.min(240, waitMs)));
          }

          harvest(records);
          const finalRows = getRowCandidates();
          const taskHydrateLimit = Math.min(120, Math.max(24, Number(pageSize) * 3));
          const desiredPageBudget = Math.max(1, Number(desiredPage || 1));
          const resultButtonLimit = desiredPageBudget > 1 ? 3 : 6;
          const resultButtonBudgetMs = desiredPageBudget > 1 ? 1600 : 3000;
          const activePageBeforeEnrich = getCurrentPageFromPagination(paginationContainers);
          pushDebug('pagination:rows-collected', {
            rowCount: finalRows.length,
            recordCount: records.size,
            taskHydrateLimit,
            resultButtonLimit,
            resultButtonBudgetMs,
            desiredPage,
            activePageBeforeEnrich
          });
          if (!fastRowsOnly) {
            await enrichRowsFromTaskApi(records, taskHydrateLimit);
            const missingBeforeButtons = Array.from(records.values()).filter((row) => {
              const current = Array.isArray(row?.resultUrls) ? row.resultUrls : [];
              return current.length === 0;
            }).length;
            if (missingBeforeButtons > 0) {
              await enrichRowsFromResultButtons(finalRows, records, resultButtonLimit, resultButtonBudgetMs);
            } else {
              pushDebug('pagination:result-buttons-skip', { reason: 'all-rows-have-result-urls' });
            }
          } else {
            pushDebug('pagination:enrich-skipped', {
              fastRowsOnly: true,
              reason: 'renderer-will-hydrate-task-ids'
            });
          }
          const ordered = Array.from(records.values())
            .sort((a, b) => {
              const aStamp = Number(a.updateTime || a.createTime || 0);
              const bStamp = Number(b.updateTime || b.createTime || 0);
              if (bStamp !== aStamp) return bStamp - aStamp;
              return Number(a._order || 0) - Number(b._order || 0);
            })
            .map((row) => {
              const out = { ...row };
              const prioritizedResultUrls = sortMediaUrls(Array.isArray(out.resultUrls) ? out.resultUrls : []);
              const prioritizedUrls = sortMediaUrls(Array.isArray(out.urls) ? out.urls : []);
              out.resultUrls = prioritizedResultUrls;
              out.urls = prioritizedResultUrls.length ? prioritizedResultUrls : prioritizedUrls;
              delete out._order;
              delete out._sig;
              return out;
            });

          const activePage = getCurrentPageFromPagination(paginationContainers);
          const hasMore = getHasNextPageFromPagination(paginationContainers);
          const pages = Math.max(1, getTotalPagesFromPagination(paginationContainers), activePage, desiredPage);
          const safeRows = ordered.slice(0, Math.max(1, Number(pageSize) || 10));
          const safePageSize = Math.max(1, Number(pageSize) || 10);
          const total = hasMore
            ? Math.max((activePage + 1) * safePageSize, pages * safePageSize)
            : Math.max(0, (activePage - 1) * safePageSize) + safeRows.length;
          pushDebug('pagination:done', {
            activePage,
            pages,
            rows: safeRows.length,
            hasMore
          });

          return {
            ok: true,
            mode: 'scrape',
            data: {
              records: safeRows,
              total,
              pages,
              hasMore,
              debug
            }
          };
        }
        pushDebug('fallback:scroll-mode');

      let lastCount = 0;
      let staleSteps = 0;
      let rows = harvest(records);
      let scrollContainer = getScrollableContainer(rows);

      for (let step = 0; step < maxScrollSteps; step += 1) {
        rows = harvest(records);
        if (records.size > lastCount) {
          staleSteps = 0;
          lastCount = records.size;
        } else {
          staleSteps += 1;
        }

        if (records.size >= targetCount && staleSteps >= 3) break;
        const nearTarget = records.size >= Math.max(pageSize, Math.floor(targetCount * 0.7));
        if (staleSteps >= 14 && nearTarget) break;
        if (staleSteps >= 24) break;

        scrollContainer = getScrollableContainer(rows);
        const primary = scrollContainer;
        const secondary = [document.scrollingElement, document.documentElement, document.body].filter(
          (el) => el && el !== primary
        );
        const containers = [primary, ...secondary].filter(Boolean);
        containers.forEach((container, idx) => {
          const currentTop = getScrollTop(container);
          const maxTop = getScrollMax(container);
          if (maxTop <= 0) return;
          const increment = Math.max(
            280,
            Math.floor((container?.clientHeight || window.innerHeight || 700) * (idx === 0 ? 0.9 : 0.55))
          );
          const nextTop = Math.min(maxTop, currentTop + increment);
          setScrollTop(container, nextTop);
        });
        await sleep(waitMs);

        const afterTop = getScrollTop(primary);
        const maxTop = getScrollMax(primary);
        const nearBottom = maxTop - afterTop <= 8;
        if (nearBottom && maxTop > 0) {
          setScrollTop(primary, Math.max(0, afterTop - 120));
          await sleep(80);
          setScrollTop(primary, maxTop);
          await sleep(waitMs + 140);
        }
      }

      harvest(records);
      const finalRows = getRowCandidates();
      const taskHydrateLimit = Math.min(220, Math.max(40, Number(targetCount || 0) + 80));
      if (!fastRowsOnly) {
        await enrichRowsFromTaskApi(records, taskHydrateLimit);
        await enrichRowsFromResultButtons(finalRows, records, 18);
      } else {
        pushDebug('fallback:enrich-skipped', {
          fastRowsOnly: true,
          reason: 'renderer-will-hydrate-task-ids'
        });
      }
      const ordered = Array.from(records.values())
        .sort((a, b) => {
          const aStamp = Number(a.updateTime || a.createTime || 0);
          const bStamp = Number(b.updateTime || b.createTime || 0);
          if (bStamp !== aStamp) return bStamp - aStamp;
          return Number(a._order || 0) - Number(b._order || 0);
        })
        .map((row) => {
          const out = { ...row };
          const prioritizedResultUrls = sortMediaUrls(Array.isArray(out.resultUrls) ? out.resultUrls : []);
          const prioritizedUrls = sortMediaUrls(Array.isArray(out.urls) ? out.urls : []);
          out.resultUrls = prioritizedResultUrls;
          out.urls = prioritizedResultUrls.length ? prioritizedResultUrls : prioritizedUrls;
          delete out._order;
          delete out._sig;
          return out;
        });

      const start = Math.max(0, (pageNum - 1) * pageSize);
      const paged = ordered.slice(start, start + pageSize);
      const total = ordered.length;
      const pages = Math.max(1, Math.ceil(total / pageSize));

        pushDebug('fallback:done', {
          rows: paged.length,
          total,
          pages,
          hasMore: pageNum < pages
        });
        return {
          ok: true,
          mode: 'scrape',
          data: {
            records: paged,
            total,
            pages,
            hasMore: pageNum < pages,
            debug
          }
        };
      } catch (err) {
        const message = String(err?.message || 'Kie logs scrape failed').trim();
        pushDebug('fatal', { message });
        return {
          ok: false,
          error: message || 'Kie logs scrape failed.',
          debug
        };
      }
    })();
  })();`;

  const timeoutMs = Math.max(
    28_000,
    Math.min(72_000, 24_000 + Math.max(1, Number(safePayload.pageNum) || 1) * 8000)
  );
  const liveDebug = [];
  const readLivePageDebugBuffer = async () => {
    try {
      if (!webContents || webContents.isDestroyed()) return [];
      const pageDebug = await webContents.executeJavaScript(
        `(() => {
          const key = '__kieScrapeDebugBuffer';
          const list = Array.isArray(window[key]) ? window[key] : [];
          return list.slice(-320);
        })();`,
        true
      );
      return Array.isArray(pageDebug) ? pageDebug : [];
    } catch (_err) {
      return [];
    }
  };
  const pushLiveDebug = (entry = {}, { level = 'info' } = {}) => {
    const normalized = entry && typeof entry === 'object' ? entry : {};
    liveDebug.push(normalized);
    if (liveDebug.length > 420) liveDebug.shift();
    emitKieLogsScrapeProgress(sender, {
      stage: String(normalized.event || 'scrape-debug'),
      level,
      detail: String(normalized.event || 'scrape-debug'),
      meta: normalized
    });
  };
  const onConsoleMessage = (_event, _level, message) => {
    const text = String(message || '').trim();
    if (!text.startsWith('[KieScrape]')) return;
    const raw = text.slice('[KieScrape]'.length).trim();
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw);
      pushLiveDebug(parsed, { level: 'info' });
    } catch (_err) {
      pushLiveDebug({ event: 'scrape-console', raw }, { level: 'info' });
    }
  };
  if (webContents && !webContents.isDestroyed()) {
    webContents.on('console-message', onConsoleMessage);
  }
  emitKieLogsScrapeProgress(sender, {
    stage: 'query-dispatched',
    level: 'info',
    detail: `Scrape query dispatched for page ${Number(safePayload.pageNum || 1)}.`,
    meta: { ...safePayload, timeoutMs }
  });
  let timeoutHandle = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(new Error(`Kie logs query timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  let result = null;
  try {
    result = await Promise.race([webContents.executeJavaScript(script, true), timeoutPromise]);
  } catch (err) {
    const message = String(err?.message || '').trim();
    if (/timed out/i.test(message)) {
      const pageDebug = await readLivePageDebugBuffer();
      const merged = [...liveDebug, ...pageDebug].filter(Boolean);
      const timeoutError = new Error(message || `Kie logs query timed out after ${timeoutMs}ms`);
      timeoutError.kieDebug = merged.slice(-320);
      timeoutError.kieScrapeError = message || 'Kie logs query timed out';
      emitKieLogsScrapeProgress(sender, {
        stage: 'query-timeout',
        level: 'error',
        detail: timeoutError.message,
        meta: {
          timeoutMs,
          pageNum: Number(safePayload.pageNum || 1),
          pageSize: Number(safePayload.pageSize || 10),
          liveDebugCount: merged.length
        }
      });
      throw timeoutError;
    }
    throw err;
  } finally {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    if (webContents && !webContents.isDestroyed()) {
      try {
        webContents.removeListener('console-message', onConsoleMessage);
      } catch (_err) {}
    }
  }
  const durationMs = Date.now() - startedAt;
  const rowCount = Array.isArray(result?.data?.records) ? result.data.records.length : 0;
  const pages = Number(result?.data?.pages || 0);
  const hasMore = !!result?.data?.hasMore;
  console.info('[KieLogs] query:done', {
    durationMs,
    ok: !!result?.ok,
    rowCount,
    pages,
    hasMore
  });
  if (!result?.ok) {
    const resultDebug = Array.isArray(result?.debug)
      ? result.debug
      : Array.isArray(result?.data?.debug)
        ? result.data.debug
        : [];
    const debugTrail = [...liveDebug, ...resultDebug].filter(Boolean);
    const tailDebug = debugTrail.slice(-6).map((entry) => `${entry?.event || 'event'}@${entry?.at || 0}`);
    console.warn('[KieLogs] query:failed', {
      error: String(result?.error || '').trim() || 'unknown',
      tailDebug
    });
    const detail = tailDebug.length ? ` Debug: ${tailDebug.join(' > ')}` : '';
    const error = new Error(
      (String(result?.error || 'Could not scrape kie.ai logs in this session.').trim() || 'Scrape failed') + detail
    );
    error.kieDebug = debugTrail;
    error.kieScrapeError = String(result?.error || '').trim() || 'Scrape failed';
    throw error;
  }
  emitKieLogsScrapeProgress(sender, {
    stage: 'query-complete',
    level: 'info',
    detail: `Scrape query complete for page ${Number(safePayload.pageNum || 1)}.`,
    meta: {
      durationMs,
      rows: rowCount,
      pages,
      hasMore
    }
  });
  return result;
};

const queryKieLogsPageInternal = async (payload = {}, { sourceWindow = null, sender = null } = {}) => {
  const normalized = normalizeKieLogsQueryPayload(payload);
  console.info('[KieLogs] internal:start', {
    normalized,
    hasSourceWindow: !!sourceWindow
  });
  const targetWindow =
    sourceWindow && !sourceWindow.isDestroyed() ? sourceWindow : await ensureKieLogsBridgeWindow();
  if (!targetWindow || targetWindow.isDestroyed()) {
    throw new Error('Kie logs session window is unavailable.');
  }
  const attemptQuery = () => runKieLogsQueryInWebContents(targetWindow.webContents, normalized, { sender });

  try {
    const result = await attemptQuery();
    console.info('[KieLogs] internal:success', {
      rows: Array.isArray(result?.data?.records) ? result.data.records.length : 0,
      pages: Number(result?.data?.pages || 0),
      hasMore: !!result?.data?.hasMore
    });
    return result;
  } catch (firstErr) {
    console.warn('[KieLogs] internal:first-attempt-failed', String(firstErr?.message || 'Unknown error'));
    if (sourceWindow && !sourceWindow.isDestroyed()) {
      throw firstErr;
    }
    try {
      await ensureKieLogsWindowLoaded(targetWindow, { timeoutMs: 45_000 });
      const retryResult = await attemptQuery();
      console.info('[KieLogs] internal:retry-success', {
        rows: Array.isArray(retryResult?.data?.records) ? retryResult.data.records.length : 0,
        pages: Number(retryResult?.data?.pages || 0),
        hasMore: !!retryResult?.data?.hasMore
      });
      return retryResult;
    } catch (_reloadErr) {
      console.error('[KieLogs] internal:retry-failed', String(firstErr?.message || 'Unknown error'));
      throw firstErr;
    }
  }
};

ipcMain.handle('query-kie-logs-page', async (_event, payload = {}) => {
  try {
    console.info('[KieLogs] ipc:query', payload);
    emitKieLogsScrapeProgress(_event?.sender, {
      stage: 'query-start',
      level: 'info',
      detail: `Starting scrape request for page ${Math.max(1, Number(payload?.pageNum) || 1)}.`,
      meta: payload && typeof payload === 'object' ? payload : undefined
    });
    const result = await queryKieLogsPageInternal(payload, { sender: _event?.sender });
    return {
      success: true,
      mode: String(result?.mode || 'scrape'),
      data: result?.data || {}
    };
  } catch (err) {
    console.error('[KieLogs] ipc:query-failed', err?.stack || err?.message || err);
    const debug = Array.isArray(err?.kieDebug) ? err.kieDebug : [];
    emitKieLogsScrapeProgress(_event?.sender, {
      stage: 'query-failed',
      level: 'error',
      detail: String(err?.message || 'Kie logs scrape query failed.').trim(),
      meta: {
        debugCount: debug.length
      }
    });
    return {
      success: false,
      error: err?.message || 'Could not query kie logs page.',
      data: {
        debug
      }
    };
  }
});

ipcMain.handle('connect-kie-logs-account', async (event) => {
  const startedAt = Date.now();
  const trace = [];
  const report = ({ step = '', status = 'active', detail = '', meta = undefined } = {}) => {
    const entry = {
      at: Date.now(),
      step: String(step || '').trim(),
      status: String(status || 'active').trim().toLowerCase(),
      detail: String(detail || '').trim(),
      elapsedMs: Math.max(0, Date.now() - startedAt),
      meta: meta && typeof meta === 'object' ? meta : undefined
    };
    trace.push(entry);
    emitKieLogsConnectProgress(event?.sender, entry);
  };

  report({
    step: 'connect-start',
    status: 'active',
    detail: 'Starting kie.ai background connection sequence.'
  });
  ensureKieLogsAuthCapture();
  try {
    report({
      step: 'auth-capture-bind',
      status: 'done',
      detail: 'Authorization capture is bound to kie session requests.'
    });
    const maxAttempts = 2;
    let lastErr = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      report({
        step: 'connect-attempt',
        status: 'active',
        detail: `Initializing hidden kie logs bridge (attempt ${attempt}/${maxAttempts}).`,
        meta: { attempt, maxAttempts }
      });
      try {
        await ensureKieLogsBridgeWindow({
          onProgress: (entry = {}) => {
            report({
              ...entry,
              meta: {
                ...(entry?.meta && typeof entry.meta === 'object' ? entry.meta : {}),
                attempt
              }
            });
          }
        });
        report({
          step: 'connect-attempt-success',
          status: 'done',
          detail: `Hidden bridge ready on attempt ${attempt}/${maxAttempts}.`,
          meta: { attempt, maxAttempts }
        });
        lastErr = null;
        break;
      } catch (err) {
        lastErr = err;
        const errMsg = String(err?.message || 'Unknown bridge error').trim();
        report({
          step: 'connect-attempt-failed',
          status: 'error',
          detail: `Attempt ${attempt}/${maxAttempts} failed: ${errMsg}`,
          meta: { attempt, maxAttempts }
        });
        if (attempt < maxAttempts) {
          report({
            step: 'connect-retry-reset',
            status: 'active',
            detail: 'Resetting hidden bridge window before retry...'
          });
          disposeKieLogsBridgeWindow({ reason: `connect-retry-${attempt}` });
          await new Promise((resolve) => setTimeout(resolve, 420));
          continue;
        }
      }
    }
    if (lastErr) throw lastErr;
    // Keep connect lightweight. Real validation happens in the first logs query.
    report({
      step: 'connect-ready',
      status: 'done',
      detail: 'Hidden kie logs session is ready for activity queries.'
    });
    return {
      success: true,
      mode: 'session',
      connectedAt: Date.now(),
      background: true,
      durationMs: Math.max(0, Date.now() - startedAt),
      trace
    };
  } catch (err) {
    const message = String(err?.message || '').trim();
    report({
      step: 'connect-failed',
      status: 'error',
      detail: message || 'Could not initialize hidden kie logs window.'
    });
    if (/not signed in|sign in|log in|login|session|expired|unauthori|401|forbidden|token/i.test(message)) {
      return {
        success: false,
        error: 'Background session check failed. Sign in to kie.ai in this app session, then reload activity.',
        durationMs: Math.max(0, Date.now() - startedAt),
        trace
      };
    }
    return {
      success: false,
      error: message || 'Could not connect kie.ai logs account.',
      durationMs: Math.max(0, Date.now() - startedAt),
      trace
    };
  }
});

ipcMain.handle('disconnect-kie-logs-account', async () => {
  try {
    const authSession = session.fromPartition(KIE_LOGS_AUTH_PARTITION);
    await authSession.clearStorageData({
      storages: ['cookies', 'localstorage', 'indexdb', 'serviceworkers', 'websql', 'filesystem', 'shadercache']
    });
    try {
      await authSession.clearAuthCache();
    } catch (_err) {}
    try {
      await authSession.clearCache();
    } catch (_err) {}
    disposeKieLogsBridgeWindow({ reason: 'manual-disconnect' });
    kieLogsLastBearerToken = '';
    return { success: true };
  } catch (err) {
    return {
      success: false,
      error: err?.message || 'Could not disconnect kie logs account.'
    };
  }
});

ipcMain.handle('runpod-list-pods', async (_event, payload = {}) => {
  const apiKey = sanitizeRunpodApiKey(payload?.apiKey || '');
  if (!apiKey) {
    return { success: false, error: 'RunPod API key is required.', pods: [] };
  }
  const queryAttempts = [RUNPOD_LIST_PODS_QUERY, RUNPOD_LIST_PODS_QUERY_FALLBACK];
  const errors = [];
  for (const query of queryAttempts) {
    try {
      const data = await runpodGraphqlRequest(apiKey, query, {});
      const pods = extractRunpodPods(data);
      return { success: true, pods };
    } catch (err) {
      errors.push(String(err?.message || 'Unknown RunPod error'));
    }
  }
  return {
    success: false,
    error: errors[0] || 'Could not load RunPod pods.',
    debug: errors,
    pods: []
  };
});

ipcMain.handle('runpod-get-pod', async (_event, payload = {}) => {
  const apiKey = sanitizeRunpodApiKey(payload?.apiKey || '');
  const podId = String(payload?.podId || '').trim();
  if (!apiKey) return { success: false, error: 'RunPod API key is required.' };
  if (!podId) return { success: false, error: 'Pod ID is required.' };
  try {
    const data = await runpodGraphqlRequest(apiKey, RUNPOD_GET_POD_QUERY, { podId });
    const pod = normalizeRunpodPod(data?.pod || null);
    if (pod) return { success: true, pod };
  } catch (_err) {}
  try {
    const data = await runpodGraphqlRequest(apiKey, RUNPOD_LIST_PODS_QUERY, {});
    const pods = extractRunpodPods(data);
    const match = pods.find((entry) => String(entry?.id || '').trim() === podId);
    if (match) return { success: true, pod: match };
    return { success: false, error: `Pod "${podId}" not found in your account.` };
  } catch (err) {
    return { success: false, error: String(err?.message || 'Could not load pod details.').trim() || 'Could not load pod details.' };
  }
});

ipcMain.handle('runpod-probe-url', async (_event, payload = {}) => {
  const targetUrl = sanitizeHttpUrl(payload?.url || '');
  if (!targetUrl) return { success: false, error: 'A valid http(s) URL is required.' };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12_000);
  try {
    const res = await fetch(targetUrl, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'User-Agent': 'Content-Studio/1.0'
      }
    });
    try {
      await res.body?.cancel();
    } catch (_err) {}
    return {
      success: true,
      ok: res.ok,
      status: Number(res.status || 0),
      finalUrl: String(res.url || targetUrl)
    };
  } catch (err) {
    if (err?.name === 'AbortError') {
      return { success: false, error: 'Probe timed out.' };
    }
    return { success: false, error: String(err?.message || 'Probe failed.').trim() || 'Probe failed.' };
  } finally {
    clearTimeout(timeout);
  }
});

ipcMain.handle('open-external-url', async (_event, payload = {}) => {
  const targetUrl = sanitizeHttpUrl(payload?.url || '');
  if (!targetUrl) return { success: false, error: 'A valid http(s) URL is required.' };
  try {
    await shell.openExternal(targetUrl);
    return { success: true };
  } catch (err) {
    return { success: false, error: String(err?.message || 'Could not open URL.').trim() || 'Could not open URL.' };
  }
});

const guessMime = (fileName) => {
  const ext = path.extname(fileName || '').toLowerCase();
  if (ext === '.gif') return 'image/gif';
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.svg') return 'image/svg+xml';
  if (ext === '.mp4') return 'video/mp4';
  if (ext === '.mov') return 'video/quicktime';
  if (ext === '.webm') return 'video/webm';
  if (ext === '.mp3') return 'audio/mpeg';
  if (ext === '.wav') return 'audio/wav';
  if (ext === '.m4a') return 'audio/mp4';
  if (ext === '.ogg') return 'audio/ogg';
  if (ext === '.pdf') return 'application/pdf';
  if (ext === '.json') return 'application/json';
  if (ext === '.txt') return 'text/plain';
  if (ext === '.zip') return 'application/zip';
  return 'application/octet-stream';
};

const normalizeUploadPath = (value = '') => {
  const trimmed = String(value || '').replace(/^\/+|\/+$/g, '');
  return trimmed || DEFAULT_UPLOAD_PATH;
};

const sanitizeFileBase = (value = '') =>
  String(value || 'upload')
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80) || 'upload';

const buildUploadFileName = (originalName = '') => {
  const ext = path.extname(originalName || '');
  const base = sanitizeFileBase(path.basename(originalName || 'upload', ext));
  const unique = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  return `${base}_${unique}${ext}`;
};

const uploadBuffer = async (buffer, fileName, { apiKey, uploadPath, mime } = {}) => {
  const key = (apiKey || process.env.KIE_API_KEY || '').trim();
  if (!key) {
    throw new Error('Missing Kie API key for upload.');
  }
  const safeUploadPath = normalizeUploadPath(uploadPath);
  const uploadName = buildUploadFileName(fileName || 'upload.bin');
  const contentType = mime || guessMime(fileName);
  const form = new FormData();
  const blob = new Blob([buffer], { type: contentType });
  form.append('file', blob, uploadName);
  form.append('uploadPath', safeUploadPath);
  form.append('fileName', uploadName);

  const res = await fetch(FILE_STREAM_UPLOAD_URL, {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}` },
    body: form
  });

  const text = await res.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch (_err) {
    payload = null;
  }

  if (!res.ok || !payload?.success) {
    const message = payload?.msg || res.statusText || 'Upload failed';
    throw new Error(`File upload failed: ${message}`);
  }
  const data = payload?.data || {};
  if (!data.downloadUrl) {
    throw new Error('Upload response missing download URL.');
  }
  return { url: data.downloadUrl, remotePath: data.filePath || '', name: data.fileName || uploadName };
};

const uploadFileStream = async (filePath, options = {}) => {
  const fileName = path.basename(filePath);
  const buffer = fs.readFileSync(filePath);
  return uploadBuffer(buffer, fileName, options);
};

ipcMain.handle('upload-files', async (_event, payload = {}) => {
  const isLegacy = Array.isArray(payload);
  const filePaths = isLegacy ? payload : payload.filePaths || [];
  const apiKey = isLegacy ? '' : payload.apiKey || '';
  const uploadPath = isLegacy ? '' : payload.uploadPath || '';
  const results = [];
  for (const filePath of filePaths) {
    try {
      const { url, remotePath, name } = await uploadFileStream(filePath, { apiKey, uploadPath });
      results.push({ path: filePath, url, remotePath, name, success: true });
    } catch (err) {
      results.push({ path: filePath, success: false, error: err.message });
    }
  }
  return results;
});

ipcMain.handle('upload-blobs', async (_event, payload = {}) => {
  const isLegacy = Array.isArray(payload);
  const files = isLegacy ? payload : payload.files || [];
  const apiKey = isLegacy ? '' : payload.apiKey || '';
  const uploadPath = isLegacy ? '' : payload.uploadPath || '';
  const results = [];
  for (const file of files) {
    try {
      const buffer = Buffer.from(file.data);
      const { url, remotePath, name } = await uploadBuffer(buffer, file.name || 'upload.bin', {
        apiKey,
        uploadPath,
        mime: file.type || ''
      });
      results.push({ name: file.name || name, url, remotePath, success: true });
    } catch (err) {
      results.push({ name: file.name, success: false, error: err.message });
    }
  }
  return results;
});

ipcMain.handle('delete-files', async (_event, remotePaths = []) => {
  return remotePaths.map((remotePath) => ({
    path: remotePath,
    success: true,
    skipped: true,
    reason: 'Temporary uploads expire automatically.'
  }));
});

ipcMain.on('renderer-log', (_event, payload = {}) => {
  const { level = 'log', message = '' } = payload;
  const fn = console[level] || console.log;
  fn(`[renderer] ${message}`);
  appendDebugTrace('renderer', message);
});

const runAlignmentScript = async ({ audioPath }) => {
  const scriptPath = path.join(__dirname, 'scripts', 'whisperx_align.py');
  if (!fs.existsSync(scriptPath)) {
    return { success: false, error: 'Alignment script not found.' };
  }

  const runWithPython = (bin) =>
    new Promise((resolve) => {
      const child = spawn(bin, [scriptPath, '--audio', audioPath], {
        stdio: ['ignore', 'pipe', 'pipe']
      });
      let stdout = '';
      let stderr = '';
      child.stdout.on('data', (data) => {
        stdout += data.toString();
      });
      child.stderr.on('data', (data) => {
        stderr += data.toString();
      });
      child.on('error', (err) => resolve({ error: err, stdout, stderr }));
      child.on('close', (code) => resolve({ code, stdout, stderr }));
    });

  let result = await runWithPython('python3');
  if (result?.error && result.error.code === 'ENOENT') {
    result = await runWithPython('python');
  }

  if (result?.error && result.error.code === 'ENOENT') {
    return { success: false, error: 'Python not found. Install python3 to use local alignment.' };
  }
  const raw = (result?.stdout || '').trim();
  if (result?.code && result.code !== 0) {
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        if (parsed?.error) {
          return { success: false, error: parsed.error };
        }
      } catch (_err) {}
    }
    return { success: false, error: result.stderr || `Alignment failed (code ${result.code}).` };
  }
  if (!raw) {
    return { success: false, error: 'No alignment output received.' };
  }
  try {
    const alignment = JSON.parse(raw);
    return { success: true, alignment };
  } catch (err) {
    return { success: false, error: `Failed to parse alignment JSON. ${err.message}` };
  }
};

ipcMain.handle('persist-blob-file', async (_event, payload = {}) => {
  const { name = 'upload.bin', data = [] } = payload;
  try {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
    const buffer = Buffer.from(data);
    const stamp = Date.now();
    const safeName = name.replace(/[^\w.-]/g, '_');
    const filePath = path.join(CACHE_DIR, `${stamp}-${safeName}`);
    fs.writeFileSync(filePath, buffer);
    return { success: true, path: filePath };
  } catch (err) {
    return { success: false, error: err.message };
  }
});

ipcMain.handle('recover-localstorage-array-snapshot', async (_event, storageKey = '') => {
  try {
    return { success: true, items: recoverLocalStorageArraySnapshot(storageKey) };
  } catch (err) {
    return { success: false, error: err.message || 'Recovery failed.', items: [] };
  }
});

ipcMain.on('load-saved-projects-sync', (event) => {
  try {
    event.returnValue = { success: true, items: loadSavedProjectsFromDisk() };
  } catch (err) {
    event.returnValue = { success: false, error: err.message || 'Load failed.', items: [] };
  }
});

ipcMain.on('persist-saved-projects-sync', (event, projects = []) => {
  try {
    persistSavedProjectsToDisk(projects);
    event.returnValue = { success: true };
  } catch (err) {
    event.returnValue = { success: false, error: err.message || 'Persist failed.' };
  }
});

ipcMain.handle('run-local-alignment', async (_event, payload = {}) => {
  const audioPath = payload.audioPath || '';
  if (!audioPath) return { success: false, error: 'No audio path provided.' };
  if (!fs.existsSync(audioPath)) return { success: false, error: 'Audio file not found.' };
  return runAlignmentScript({ audioPath });
});

ipcMain.handle('read-local-file', async (_event, filePath = '') => {
  if (!filePath) return { success: false, error: 'No file path provided' };
  try {
    const buffer = fs.readFileSync(filePath);
    const mime = guessMime(filePath);
    const dataUrl = `data:${mime};base64,${buffer.toString('base64')}`;
    return { success: true, dataUrl, mime };
  } catch (err) {
    return { success: false, error: err.message };
  }
});

ipcMain.handle('path-exists', async (_event, filePath = '') => {
  if (!filePath) return { success: false, exists: false };
  try {
    return { success: true, exists: fs.existsSync(filePath) };
  } catch (_err) {
    return { success: false, exists: false };
  }
});

ipcMain.handle('cache-remote-file', async (_event, payload = {}) => {
  const { url = '', sceneId = '', kind = 'media' } = payload;
  if (!url) return { success: false, error: 'No URL provided.' };
  try {
    const parsed = new URL(url);
    const extFromPath = path.extname(parsed.pathname || '').replace('.', '').toLowerCase();
    const defaultExt = kind === 'image' ? 'jpg' : 'mp4';
    const ext = extFromPath && /^[a-z0-9]{2,5}$/.test(extFromPath) ? extFromPath : defaultExt;
    fs.mkdirSync(TIKTOK_MEDIA_CACHE_DIR, { recursive: true });
    const safeScene = String(sceneId || kind || 'media').replace(/[^\w.-]+/g, '_');
    const outPath = path.join(TIKTOK_MEDIA_CACHE_DIR, `${safeScene}-${Date.now()}.${ext}`);
    const res = await fetch(url, { redirect: 'follow' });
    if (!res.ok) {
      return { success: false, error: `Download failed (HTTP ${res.status}).` };
    }
    const buffer = Buffer.from(await res.arrayBuffer());
    fs.writeFileSync(outPath, buffer);
    return { success: true, path: outPath };
  } catch (err) {
    return { success: false, error: err.message || 'Cache failed.' };
  }
});

ipcMain.handle('reveal-in-folder', async (_event, filePath = '') => {
  const raw = String(filePath || '').trim();
  if (!raw) return { success: false, error: 'No file path provided.' };
  try {
    let target = raw;
    if (/^file:\/\//i.test(target)) {
      try {
        target = decodeURIComponent(new URL(target).pathname || '');
      } catch (_err) {}
      if (process.platform === 'win32' && /^\/[A-Za-z]:/.test(target)) {
        target = target.slice(1);
      }
    }
    if (!fs.existsSync(target)) {
      return { success: false, error: 'File not found.' };
    }
    shell.showItemInFolder(target);
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message || 'Unable to reveal file.' };
  }
});

ipcMain.handle('split-audio', async (_event, payload = {}) => {
  const audioPath = payload.audioPath || '';
  const segments = Array.isArray(payload.segments) ? payload.segments : [];
  if (!audioPath) return { success: false, error: 'No audio path provided.' };
  if (!fs.existsSync(audioPath)) return { success: false, error: 'Audio file not found.' };
  if (!segments.length) return { success: false, error: 'No segments provided.' };

  const runFfmpeg = (args) =>
    new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', sanitizeFfmpegArgs(args), { stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      proc.stderr.on('data', (data) => {
        err += data.toString();
      });
      proc.on('error', (error) => reject(error));
      proc.on('close', (code) => {
        if (code === 0) resolve();
        else reject(new Error(err || `ffmpeg exited with ${code}`));
      });
    });

  fs.mkdirSync(TIKTOK_AUDIO_SEGMENTS_CACHE_DIR, { recursive: true });
  const tmpRoot = fs.mkdtempSync(path.join(TIKTOK_AUDIO_SEGMENTS_CACHE_DIR, 'tiktok-audio-'));
  const results = [];
  for (let i = 0; i < segments.length; i += 1) {
    const seg = segments[i] || {};
    const startMs = Number(seg.start_ms || 0);
    const endMs = Number(seg.end_ms || 0);
    const durationMs = Math.max(200, endMs - startMs);
    const startSec = Math.max(0, startMs / 1000);
    const durationSec = Math.max(0.2, durationMs / 1000);
    const id = String(seg.id || `seg-${i + 1}`);
    const safeId = id.replace(/[^\w.-]+/g, '_');
    const outPath = path.join(tmpRoot, `${safeId}.m4a`);
    try {
      await runFfmpeg([
        '-ss',
        String(startSec),
        '-t',
        String(durationSec),
        '-i',
        audioPath,
        '-vn',
        '-c:a',
        'aac',
        '-b:a',
        '192k',
        '-y',
        outPath
      ]);
      results.push({ id, path: outPath, start_ms: startMs, end_ms: endMs, success: true });
    } catch (err) {
      results.push({ id, path: outPath, start_ms: startMs, end_ms: endMs, success: false, error: err.message });
    }
  }

  const failed = results.filter((r) => !r.success);
  if (failed.length) {
    return { success: false, error: `Failed ${failed.length} audio segment(s).`, segments: results };
  }
  return { success: true, segments: results };
});

ipcMain.handle('transcribe-openai-audio', async (_event, payload = {}) => {
  const apiKey = String(payload.apiKey || '').trim();
  const audioPath = String(payload.audioPath || '').trim();
  const model = String(payload.model || 'gpt-4o-mini-transcribe').trim() || 'gpt-4o-mini-transcribe';
  if (!apiKey) return { success: false, error: 'Missing OpenAI API key.' };
  if (!audioPath) return { success: false, error: 'No audio/video path provided.' };
  if (!fs.existsSync(audioPath)) return { success: false, error: 'Audio/video file not found.' };

  try {
    const fileName = path.basename(audioPath);
    const buffer = fs.readFileSync(audioPath);
    const callTranscribe = async ({ responseFormat = 'verbose_json', includeWordTimestamps = false } = {}) => {
      const form = new FormData();
      form.append('file', new Blob([buffer], { type: guessMime(fileName) }), fileName);
      form.append('model', model);
      form.append('response_format', responseFormat);
      if (includeWordTimestamps) {
        form.append('timestamp_granularities[]', 'word');
      }
      const res = await fetch('https://api.openai.com/v1/audio/transcriptions', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`
        },
        body: form
      });
      const text = await res.text();
      let parsed = {};
      try {
        parsed = text ? JSON.parse(text) : {};
      } catch (_err) {
        parsed = {};
      }
      return {
        ok: res.ok,
        status: res.status,
        parsed,
        error: parsed?.error?.message || `Transcription failed (${res.status})`
      };
    };

    let parsed = {};
    const verboseAttempt = await callTranscribe({ responseFormat: 'verbose_json', includeWordTimestamps: true });
    if (verboseAttempt.ok) {
      parsed = verboseAttempt.parsed || {};
    } else {
      const errMsg = String(verboseAttempt.error || '');
      const needsJsonFallback = /response_format|not compatible|verbose_json/i.test(errMsg);
      if (!needsJsonFallback) {
        return { success: false, error: errMsg || 'Transcription failed.' };
      }
      const jsonAttempt = await callTranscribe({ responseFormat: 'json', includeWordTimestamps: false });
      if (!jsonAttempt.ok) {
        return { success: false, error: String(jsonAttempt.error || errMsg || 'Transcription failed.') };
      }
      parsed = jsonAttempt.parsed || {};
    }
    const transcript = String(parsed?.text || '').trim();
    const wordsRaw = Array.isArray(parsed?.words) ? parsed.words : [];
    const words = wordsRaw
      .map((word, idx) => {
        const textValue = String(word?.word || word?.text || '').trim();
        const start = Number(word?.start ?? word?.start_sec ?? (Number(word?.start_ms) || 0) / 1000);
        const end = Number(word?.end ?? word?.end_sec ?? (Number(word?.end_ms) || 0) / 1000);
        if (!textValue) return null;
        const safeStart = Number.isFinite(start) ? Math.max(0, start) : idx * 0.25;
        const safeEnd = Number.isFinite(end) && end > safeStart ? end : safeStart + 0.25;
        return { word: textValue, start: safeStart, end: safeEnd };
      })
      .filter(Boolean);

    return {
      success: true,
      text: transcript,
      words
    };
  } catch (err) {
    return { success: false, error: err.message || 'Transcription failed.' };
  }
});

ipcMain.handle('render-tiktok-captioned-video', async (_event, payload = {}) => {
  const failCaptionRender = (message, { includePipelineVersion = false } = {}) => {
    const base = String(message || 'Subtitle render failed.').trim() || 'Subtitle render failed.';
    const full = includePipelineVersion ? `[${CAPTION_RENDER_PIPELINE_VERSION}] ${base}` : base;
    return {
      success: false,
      error: full,
      errorCopied: copyTextToClipboardSafe(full)
    };
  };

  const videoPath = String(payload.videoPath || '').trim();
  const wordsRaw = Array.isArray(payload.words) ? payload.words : [];
  const width = Number(payload.width || 1080);
  const height = Number(payload.height || 1920);
  const outputNameRaw = String(payload.outputName || '').trim();
  const renderPlanRaw = payload.renderPlan && typeof payload.renderPlan === 'object' ? payload.renderPlan : null;
  const progressRunId = String(payload.progressRunId || '').trim();
  const progressDurationSecRaw = Number(payload.durationSec || 0);
  if (!videoPath) return failCaptionRender('No source video path provided.');
  if (!fs.existsSync(videoPath)) return failCaptionRender('Source video not found.');
  if (!wordsRaw.length) return failCaptionRender('No timed words provided for subtitle render.');

  const normalizeWord = (value = {}, idx = 0) => {
    const text = String(value?.word || value?.text || '').trim();
    const start = Number(value?.start ?? value?.start_sec ?? (Number(value?.start_ms) || 0) / 1000);
    const end = Number(value?.end ?? value?.end_sec ?? (Number(value?.end_ms) || 0) / 1000);
    if (!text) return null;
    const safeStart = Number.isFinite(start) ? Math.max(0, start) : idx * 0.25;
    const safeEnd = Number.isFinite(end) && end > safeStart ? end : safeStart + 0.25;
    return { word: text, start: safeStart, end: safeEnd };
  };
  const words = wordsRaw.map((word, idx) => normalizeWord(word, idx)).filter(Boolean).sort((a, b) => a.start - b.start);
  if (!words.length) return failCaptionRender('Timed words are empty after normalization.');
  const progressDurationSec = progressDurationSecRaw > 0 ? progressDurationSecRaw : Number(words[words.length - 1]?.end || 0);
  const introOverlayRaw =
    payload.introAvatarOverlay && typeof payload.introAvatarOverlay === 'object' ? payload.introAvatarOverlay : null;
  const parseFileUrlPath = (value = '') => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    if (!/^file:\/\//i.test(raw)) return raw;
    try {
      return decodeURIComponent(new URL(raw).pathname || '');
    } catch (_err) {
      return raw;
    }
  };
  const clampNumeric = (value, min, max, fallback) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(min, Math.min(max, n));
  };
  const introOverlay = (() => {
    if (!introOverlayRaw) return null;
    const rawUrl = String(introOverlayRaw.url || '').trim();
    const parsedUrl = parseFileUrlPath(rawUrl);
    if (!parsedUrl) return null;
    const isHttp = /^https?:\/\//i.test(parsedUrl);
    if (!isHttp && !fs.existsSync(parsedUrl)) return null;
    const parsedMaskPath = parseFileUrlPath(String(introOverlayRaw.maskPath || '').trim());
    const hasMaskPath = !!parsedMaskPath && !/^https?:\/\//i.test(parsedMaskPath) && fs.existsSync(parsedMaskPath);
    const startSec = Math.max(0, Number(introOverlayRaw.startSec || 0));
    const endSec = Math.max(startSec + 0.08, Number(introOverlayRaw.endSec || startSec + 0.8));
    if (endSec <= startSec + 0.05) return null;
    const modeRaw = String(introOverlayRaw.mode || 'greenscreen_black').toLowerCase();
    const mode = modeRaw === 'greenscreen' ? 'greenscreen' : 'greenscreen_black';
    const scale = clampNumeric(introOverlayRaw.scale, 0.18, 0.85, 0.52);
    const centerXPercent = clampNumeric(introOverlayRaw.centerXPercent, 0, 100, 50);
    const marginBottomPercent = clampNumeric(introOverlayRaw.marginBottomPercent, 0, 0.35, 0.03);
    const greenSimilarity = clampNumeric(introOverlayRaw.greenSimilarity, 0.05, 0.45, 0.24);
    const greenBlend = clampNumeric(introOverlayRaw.greenBlend, 0, 0.2, 0.06);
    const blackSimilarity = clampNumeric(introOverlayRaw.blackSimilarity, 0.005, 0.2, 0.04);
    const blackBlend = clampNumeric(introOverlayRaw.blackBlend, 0, 0.12, 0.01);
    const isImage = /\.(png|jpe?g|webp|bmp)$/i.test(String(parsedUrl).split('?')[0] || '');
    const sourceType = String(introOverlayRaw.sourceType || '').trim().toLowerCase();
    const detectForeground = introOverlayRaw.detectForeground !== false && sourceType === 'scene_avatar_video' && !isImage;
    return {
      url: parsedUrl,
      isImage,
      sourceType,
      detectForeground,
      maskPath: hasMaskPath ? parsedMaskPath : '',
      fullFrame: !!introOverlayRaw.fullFrame && hasMaskPath,
      startSec,
      endSec,
      mode,
      scale,
      centerXPercent,
      marginBottomPercent,
      greenSimilarity,
      greenBlend,
      blackSimilarity,
      blackBlend
    };
  })();
  const sendCaptionRenderProgress = (percent = 0, message = '') => {
    if (!progressRunId) return;
    const webContents = _event?.sender;
    if (!webContents || webContents.isDestroyed()) return;
    try {
      webContents.send('tiktok-caption-render-progress', {
        runId: progressRunId,
        percent: Math.max(0, Math.min(100, Number(percent) || 0)),
        message: String(message || '')
      });
    } catch (_err) {}
  };

  const clampStyle = (value, min, max, fallback) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return fallback;
    return Math.max(min, Math.min(max, parsed));
  };
  const normalizeHex = (value, fallback) => {
    const raw = String(value || '').trim();
    return /^#[0-9a-f]{6}$/i.test(raw) ? raw.toUpperCase() : fallback;
  };
  const normalizeRenderOp = (value = {}, order = 0) => {
    const text = String(value?.text || '').trim();
    const startRaw = Number(value?.start);
    const endRaw = Number(value?.end);
    const xRaw = Number(value?.x);
    const yRaw = Number(value?.y);
    const fontSizeRaw = Number(value?.fontSize);
    if (!text || !Number.isFinite(startRaw) || !Number.isFinite(endRaw)) return null;
    if (!Number.isFinite(xRaw) || !Number.isFinite(yRaw) || !Number.isFinite(fontSizeRaw)) return null;
    const start = Math.max(0, startRaw);
    const end = Math.max(start + 0.04, endRaw);
    const layerRaw = Number(value?.layer);
    const layer = Number.isFinite(layerRaw) ? Math.max(0, Math.min(2, Math.round(layerRaw))) : 0;
    return {
      order: Math.max(0, Number(order) || 0),
      layer,
      text,
      start,
      end,
      x: Math.max(0, xRaw),
      y: Math.max(0, yRaw),
      fontSize: Math.max(12, fontSizeRaw),
      color: normalizeHex(value?.color, '#FFFFFF')
    };
  };
  const renderPlanOpsRaw = Array.isArray(renderPlanRaw?.ops) ? renderPlanRaw.ops : [];
  const renderPlanOps = renderPlanOpsRaw
    .map((entry, idx) => normalizeRenderOp(entry, idx))
    .filter(Boolean);
  const toAssColor = (hex, alpha = 0) => {
    const normalized = normalizeHex(hex, '#FFFFFF').replace('#', '');
    const r = normalized.slice(0, 2);
    const g = normalized.slice(2, 4);
    const b = normalized.slice(4, 6);
    const a = Math.max(0, Math.min(255, Math.round(alpha)));
    return `&H${a.toString(16).padStart(2, '0').toUpperCase()}${b}${g}${r}`;
  };
  const rawStyle = payload.overlayStyle && typeof payload.overlayStyle === 'object' ? payload.overlayStyle : {};
  const chunkOverrides =
    payload.chunkOverrides && typeof payload.chunkOverrides === 'object'
      ? payload.chunkOverrides
      : {};
  const style = {
    fontFamily:
      String(rawStyle.fontFamily || 'Arial')
        .replace(/[\r\n,]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim() || 'Arial',
    fontSize: Math.round(clampStyle(rawStyle.fontSize, 28, 140, 72)),
    textColor: normalizeHex(rawStyle.textColor, '#FFFFFF'),
    highlightColor: normalizeHex(rawStyle.highlightColor, '#FFA500'),
    outlineColor: normalizeHex(rawStyle.outlineColor, '#101010'),
    outlineSize: clampStyle(rawStyle.outlineSize, 0, 14, 7),
    shadow: clampStyle(rawStyle.shadow, 0, 10, 0),
    posXPercent: clampStyle(rawStyle.posXPercent, 5, 95, 50),
    posYPercent: clampStyle(rawStyle.posYPercent, 8, 95, 84),
    boxWidthPercent: Math.round(clampStyle(rawStyle.boxWidthPercent, 20, 95, 70)),
    boxHeightPercent: Math.round(clampStyle(rawStyle.boxHeightPercent, 8, 70, 20)),
    activeWordPop: rawStyle.activeWordPop !== false,
    activeWordScale: clampStyle(rawStyle.activeWordScale, 1, 1.8, 1.25),
    activeTimingOffsetMs: Math.round(clampStyle(rawStyle.activeTimingOffsetMs, -350, 350, 0)),
    bold: rawStyle.bold !== false,
    italic: !!rawStyle.italic
  };
  const posX = Math.max(1, Math.min(Math.round((style.posXPercent / 100) * Math.max(100, Math.round(width))), Math.max(100, Math.round(width)) - 1));
  const posY = Math.max(1, Math.min(Math.round((style.posYPercent / 100) * Math.max(100, Math.round(height))), Math.max(100, Math.round(height)) - 1));
  const boxWidthPx = Math.max(80, Math.round((style.boxWidthPercent / 100) * Math.max(100, Math.round(width))));
  const boxHeightPx = Math.max(44, Math.round((style.boxHeightPercent / 100) * Math.max(100, Math.round(height))));

  const escapeAss = (value = '') =>
    String(value || '')
      .replace(/\\/g, '\\\\')
      .replace(/\{/g, '\\{')
      .replace(/\}/g, '\\}')
      .replace(/\n/g, '\\N');
  const toAssTime = (sec = 0) => {
    const safe = Math.max(0, Number(sec) || 0);
    const h = Math.floor(safe / 3600);
    const m = Math.floor((safe % 3600) / 60);
    const s = Math.floor(safe % 60);
    const cs = Math.floor((safe - Math.floor(safe)) * 100);
    return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}.${String(cs).padStart(2, '0')}`;
  };
  const chunkWords = () => {
    const chunks = [];
    let current = [];
    let start = Number(words[0]?.start || 0);
    const shouldBreakAfterWord = (word, nextWord, chunkStart, chunkLen) => {
      const text = String(word?.word || '').trim();
      const hasPhrasePunct = /[.!?;,:\u2026]$/.test(text);
      const gapToNext = nextWord ? Number(nextWord.start || 0) - Number(word.end || 0) : 999;
      const span = Number(word.end || 0) - Number(chunkStart || 0);
      if (hasPhrasePunct) return true;
      if (gapToNext >= 0.34) return true;
      if (chunkLen >= 7) return true;
      if (span >= 3.3) return true;
      return false;
    };
    words.forEach((word, idx) => {
      if (!current.length) start = Number(word.start || 0);
      current.push(word);
      const nextWord = words[idx + 1];
      if (shouldBreakAfterWord(word, nextWord, start, current.length)) {
        chunks.push(current);
        current = [];
      }
    });
    if (current.length) chunks.push(current);
    return chunks;
  };
  const buildChunkRenderData = () => {
    const chunksBase = chunkWords();
    const chunks = chunksBase.map((chunk, idx) => {
      const override = String(chunkOverrides[String(idx)] || '').trim();
      if (!override) return chunk;
      const tokens = override.split(/\s+/).map((v) => v.trim()).filter(Boolean);
      if (!tokens.length) return chunk;
      return chunk.map((word, wi) => ({
        ...word,
        word: tokens[Math.min(wi, tokens.length - 1)] || word.word
      }));
    });
    const leadOffsetSec = Number(style.activeTimingOffsetMs || 0) / 1000;
    return chunks.map((chunk) => {
      const wordsShifted = chunk.map((word, idx) => {
        const nextWord = chunk[idx + 1];
        const baseStart = Number(word.start || 0);
        const baseEnd = nextWord
          ? Number(nextWord.start || baseStart + 0.2)
          : Math.max(baseStart + 0.08, Number(word.end || baseStart + 0.2));
        const start = Math.max(0, baseStart - leadOffsetSec);
        const end = Math.max(start + 0.06, baseEnd - leadOffsetSec);
        return {
          word: String(word.word || '').trim(),
          start,
          end
        };
      });
      const filteredWords = wordsShifted.filter((entry) => entry.word);
      const start = Number(filteredWords[0]?.start || 0);
      const end = Math.max(start + 0.08, Number(filteredWords[filteredWords.length - 1]?.end || start + 0.25));
      const text = filteredWords.map((entry) => entry.word).join(' ');
      return {
        start,
        end,
        text,
        words: filteredWords
      };
    });
  };
  const estimateTextWidth = (text = '', fontSize = 72) => {
    const value = String(text || '');
    let widthUnits = 0;
    for (let i = 0; i < value.length; i += 1) {
      const ch = value[i];
      if (/\s/.test(ch)) widthUnits += 0.33;
      else if (/[ilI1.,:;'"`]/.test(ch)) widthUnits += 0.28;
      else if (/[mwMW@#%&]/.test(ch)) widthUnits += 0.9;
      else widthUnits += 0.56;
    }
    return widthUnits * Number(fontSize || 72);
  };
  const layoutChunkForBox = (chunk = {}) => {
    const wordsInChunk = Array.isArray(chunk.words) ? chunk.words.filter((entry) => String(entry.word || '').trim()) : [];
    if (!wordsInChunk.length) return null;

    let fontSize = Math.max(14, Math.round(Number(style.fontSize || 72)));
    let lines = [];
    let lineHeight = Math.max(18, fontSize * 1.22);
    let blockHeight = lineHeight;
    for (let pass = 0; pass < 5; pass += 1) {
      const spaceWidth = estimateTextWidth(' ', fontSize);
      lines = [];
      let current = { words: [], width: 0 };
      wordsInChunk.forEach((entry) => {
        const word = String(entry.word || '').trim();
        if (!word) return;
        const wordWidth = estimateTextWidth(word, fontSize);
        const projected = current.words.length ? current.width + spaceWidth + wordWidth : wordWidth;
        if (current.words.length && projected > boxWidthPx) {
          lines.push(current);
          current = { words: [], width: 0 };
        }
        if (current.words.length) current.width += spaceWidth;
        current.words.push({ ...entry, word, width: wordWidth });
        current.width += wordWidth;
      });
      if (current.words.length) lines.push(current);
      if (!lines.length) break;
      lineHeight = Math.max(18, fontSize * 1.22);
      blockHeight = lineHeight * lines.length;
      if (blockHeight <= boxHeightPx || fontSize <= 14) break;
      const nextSize = Math.max(14, Math.floor(fontSize * Math.max(0.68, boxHeightPx / blockHeight)));
      if (nextSize === fontSize) break;
      fontSize = nextSize;
    }

    if (!lines.length) return null;
    const spaceWidth = estimateTextWidth(' ', fontSize);
    lineHeight = Math.max(18, fontSize * 1.22);
    blockHeight = lineHeight * lines.length;
    const topY = Math.max(0, Math.min(height - lineHeight, posY - blockHeight / 2));
    const lineRecords = lines.map((line, lineIdx) => {
      const lineY = Math.max(0, Math.min(height - lineHeight, topY + lineIdx * lineHeight));
      const lineX = Math.max(0, Math.min(width - 4, posX - line.width / 2));
      let cursorX = lineX;
      const tokens = line.words.map((word, idx) => {
        if (idx > 0) cursorX += spaceWidth;
        const token = {
          word: word.word,
          start: Number(word.start || 0),
          end: Number(word.end || 0),
          width: Number(word.width || 0),
          x: cursorX,
          y: lineY
        };
        cursorX += Number(word.width || 0);
        return token;
      });
      return {
        text: line.words.map((word) => word.word).join(' '),
        x: lineX,
        y: lineY,
        tokens
      };
    });

    return {
      fontSize,
      lines: lineRecords
    };
  };
  const buildAss = () => {
    const chunksBase = chunkWords();
    const chunks = chunksBase.map((chunk, idx) => {
      const override = String(chunkOverrides[String(idx)] || '').trim();
      if (!override) return chunk;
      const tokens = override.split(/\s+/).map((v) => v.trim()).filter(Boolean);
      if (!tokens.length) return chunk;
      return chunk.map((word, wi) => ({
        ...word,
        word: tokens[Math.min(wi, tokens.length - 1)] || word.word
      }));
    });
    const header = [
      '[Script Info]',
      'ScriptType: v4.00+',
      `PlayResX: ${Math.max(100, Math.round(width))}`,
      `PlayResY: ${Math.max(100, Math.round(height))}`,
      '',
      '[V4+ Styles]',
      'Format: Name,Fontname,Fontsize,PrimaryColour,SecondaryColour,OutlineColour,BackColour,Bold,Italic,Underline,StrikeOut,ScaleX,ScaleY,Spacing,Angle,BorderStyle,Outline,Shadow,Alignment,MarginL,MarginR,MarginV,Encoding',
      `Style: Caption,${style.fontFamily},${style.fontSize},${toAssColor(style.textColor, 0)},${toAssColor(style.highlightColor, 0)},${toAssColor(style.outlineColor, 0)},&H64000000,${style.bold ? -1 : 0},${style.italic ? -1 : 0},0,0,100,100,0,0,1,${Number(style.outlineSize).toFixed(1)},${Number(style.shadow).toFixed(1)},5,20,20,20,1`,
      '',
      '[Events]',
      'Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text'
    ];
    const events = [];
    const leadOffsetSec = Number(style.activeTimingOffsetMs || 0) / 1000;
    chunks.forEach((chunk) => {
      chunk.forEach((activeWord, activeIdx) => {
        const nextWord = chunk[activeIdx + 1];
        const baseStart = Number(activeWord.start || 0);
        const baseEnd = nextWord
          ? Number(nextWord.start || baseStart + 0.2)
          : Math.max(baseStart + 0.08, Number(activeWord.end || baseStart + 0.2));
        const start = Math.max(0, baseStart - leadOffsetSec);
        const end = Math.max(start + 0.06, baseEnd - leadOffsetSec);
        const activeFont = Math.max(12, Math.round(style.fontSize * Number(style.activeWordScale || 1.25)));
        const line = chunk
          .map((word, idx) => {
            const safeText = escapeAss(word.word);
            if (idx !== activeIdx) return safeText;
            if (!style.activeWordPop) return `{\\c${toAssColor(style.highlightColor, 0)}}${safeText}{\\rCaption}`;
            return `{\\c${toAssColor(style.highlightColor, 0)}\\fs${activeFont}}${safeText}{\\rCaption}`;
          })
          .join(' ');
        events.push(
          `Dialogue: 0,${toAssTime(start)},${toAssTime(Math.max(start + 0.08, end))},Caption,,0,0,0,,{\\an5\\q2\\blur0.6\\pos(${posX},${posY})}${line}`
        );
      });
    });
    return `${header.concat(events).join('\n')}\n`;
  };
  const escapeDrawtextValue = (value = '') =>
    String(value || '')
      .replace(/\\/g, '\\\\')
      .replace(/:/g, '\\:')
      .replace(/'/g, "\\'")
      .replace(/,/g, '\\,')
      .replace(/;/g, '\\;')
      .replace(/\[/g, '\\[')
      .replace(/\]/g, '\\]')
      .replace(/%/g, '\\%')
      .replace(/\n/g, ' ');
  const escapeDrawtextPath = (value = '') =>
    String(value || '')
      .replace(/\\/g, '\\\\')
      .replace(/:/g, '\\:')
      .replace(/'/g, "\\'")
      .replace(/,/g, '\\,')
      .replace(/;/g, '\\;')
      .replace(/%/g, '\\%');
  const toDrawtextColor = (hex = '#FFFFFF') => `0x${normalizeHex(hex, '#FFFFFF').replace('#', '')}`;
  const toDrawtextColorWithAlpha = (hex = '#FFFFFF', alpha = 1) => {
    const safeAlpha = Math.max(0, Math.min(1, Number(alpha)));
    return `${toDrawtextColor(hex)}@${safeAlpha.toFixed(3)}`;
  };
  let drawtextTextFileCounter = 0;
  const buildDrawtextFilterFromPlan = ({ includeFont = true } = {}) => {
    if (!renderPlanOps.length) return null;
    const orderedOps = [...renderPlanOps].sort((a, b) => {
      const layerDelta = Number(a.layer || 0) - Number(b.layer || 0);
      if (layerDelta !== 0) return layerDelta;
      const orderDelta = Number(a.order || 0) - Number(b.order || 0);
      if (orderDelta !== 0) return orderDelta;
      return Number(a.start || 0) - Number(b.start || 0);
    });
    const borderWidth = Math.max(0, Number(style.outlineSize || 0));
    const shadowPx = Math.max(0, Math.round(Number(style.shadow || 0)));
    const fontSuffix = style.bold && style.italic ? ' Bold Italic' : style.bold ? ' Bold' : style.italic ? ' Italic' : '';
    const fontName = escapeDrawtextValue(`${style.fontFamily || 'Sans'}${fontSuffix}`);
    const fontPart = includeFont ? `font='${fontName}':` : '';
    const writeTextFile = (text = '') => {
      const textFilePath = path.join(tmpRoot, `caption-plan-line-${drawtextTextFileCounter++}.txt`);
      fs.writeFileSync(textFilePath, String(text || ''), 'utf8');
      return escapeDrawtextPath(textFilePath);
    };
    let label = 0;
    const filters = [];
    const pushPass = ({
      safePath = '',
      fontSize = 72,
      color = '#FFFFFF',
      colorRaw = '',
      x = 0,
      y = 0,
      start = 0,
      end = 0.1,
      border = 0,
      shadow = 0
    }) => {
      const inLabel = label;
      const outLabel = label + 1;
      const entry =
        `[v${inLabel}]drawtext=` +
        fontPart +
        `textfile='${safePath}':` +
        `fontsize=${Math.max(12, Math.round(fontSize || 72))}:` +
        `fontcolor=${colorRaw || toDrawtextColor(color)}:` +
        `x=${Math.max(0, Number(x || 0)).toFixed(2)}:` +
        `y=${Math.max(0, Number(y || 0)).toFixed(2)}:` +
        `borderw=${Math.max(0, Number(border || 0)).toFixed(1)}:` +
        `bordercolor=${toDrawtextColor(style.outlineColor)}:` +
        `shadowx=${Math.max(0, Math.round(Number(shadow || 0)))}:` +
        `shadowy=${Math.max(0, Math.round(Number(shadow || 0)))}:` +
        `shadowcolor=${toDrawtextColor(style.outlineColor)}:` +
        `enable='between(t\\,${Number(start || 0).toFixed(3)}\\,${Number(end || 0).toFixed(3)})'` +
        `[v${outLabel}]`;
      filters.push(entry);
      label = outLabel;
    };
    orderedOps.forEach((op) => {
      const safePath = writeTextFile(op.text);
      const start = Number(op.start || 0);
      const end = Number(op.end || start + 0.08);
      const x = Number(op.x || 0);
      const y = Number(op.y || 0);
      const fontSize = Math.max(12, Number(op.fontSize || 72));
      // Draw the outline pass first, then fill on top so the outline stays outside the glyph.
      if (borderWidth > 0) {
        pushPass({
          safePath,
          fontSize,
          colorRaw: toDrawtextColorWithAlpha(style.outlineColor, 0),
          x,
          y,
          start,
          end,
          border: borderWidth,
          shadow: 0
        });
      }
      pushPass({
        safePath,
        fontSize,
        color: op.color,
        x,
        y,
        start,
        end,
        border: 0,
        shadow: shadowPx
      });
    });
    if (!filters.length) return null;
    return {
      expr: filters.join(';'),
      lastLabel: label
    };
  };
  const buildDrawtextFilter = ({ includeFont = true } = {}) => {
    const chunks = buildChunkRenderData().filter((chunk) => String(chunk.text || '').trim());
    if (!chunks.length) return null;
    const borderWidth = Number(style.outlineSize || 0);
    const shadowPx = Math.max(0, Math.round(Number(style.shadow || 0)));
    const fontSuffix = style.bold && style.italic ? ' Bold Italic' : style.bold ? ' Bold' : style.italic ? ' Italic' : '';
    const fontName = escapeDrawtextValue(`${style.fontFamily || 'Sans'}${fontSuffix}`);
    const fontPart = includeFont ? `font='${fontName}':` : '';
    const writeTextFile = (text = '') => {
      const textFilePath = path.join(tmpRoot, `caption-line-${drawtextTextFileCounter++}.txt`);
      fs.writeFileSync(textFilePath, String(text || ''), 'utf8');
      return escapeDrawtextPath(textFilePath);
    };
    let label = 0;
    const filters = [];
    const pushDrawtextFilter = ({
      text = '',
      start = 0,
      end = 0.1,
      x = 0,
      y = 0,
      fontSize = 72,
      color = '#FFFFFF',
      colorRaw = '',
      boldPass = false,
      border = borderWidth,
      shadow = shadowPx,
      borderColor = style.outlineColor,
      shadowColor = style.outlineColor
    }) => {
      const safePath = writeTextFile(text);
      const safeStart = Math.max(0, Number(start || 0));
      const safeEnd = Math.max(safeStart + 0.04, Number(end || safeStart + 0.1));
      const inLabel = label;
      const outLabel = label + 1;
      const dx = boldPass ? 1 : 0;
      const entry =
        `[v${inLabel}]drawtext=` +
        fontPart +
        `textfile='${safePath}':` +
        `fontsize=${Math.max(12, Math.round(fontSize || 72))}:` +
        `fontcolor=${colorRaw || toDrawtextColor(color)}:` +
        `x=${Math.max(0, Number(x || 0) + dx).toFixed(2)}:` +
        `y=${Math.max(0, Number(y || 0)).toFixed(2)}:` +
        `borderw=${Math.max(0, Number(border || 0)).toFixed(1)}:` +
        `bordercolor=${toDrawtextColor(borderColor)}:` +
        `shadowx=${Math.max(0, Math.round(Number(shadow || 0)))}:` +
        `shadowy=${Math.max(0, Math.round(Number(shadow || 0)))}:` +
        `shadowcolor=${toDrawtextColor(shadowColor)}:` +
        `enable='between(t\\,${safeStart.toFixed(3)}\\,${safeEnd.toFixed(3)})'` +
        `[v${outLabel}]`;
      filters.push(entry);
      label = outLabel;
    };
    const pushStyledText = ({
      text = '',
      start = 0,
      end = 0.1,
      x = 0,
      y = 0,
      fontSize = 72,
      color = '#FFFFFF'
    }) => {
      if (borderWidth > 0) {
        pushDrawtextFilter({
          text,
          start,
          end,
          x,
          y,
          fontSize,
          colorRaw: toDrawtextColorWithAlpha(style.outlineColor, 0),
          border: borderWidth,
          shadow: 0,
          boldPass: false
        });
      }
      pushDrawtextFilter({
        text,
        start,
        end,
        x,
        y,
        fontSize,
        color,
        border: 0,
        shadow: shadowPx,
        boldPass: false
      });
    };

    chunks.forEach((chunk) => {
      const layout = layoutChunkForBox(chunk);
      if (!layout || !Array.isArray(layout.lines) || !layout.lines.length) return;
      const chunkStart = Number(chunk.start || 0);
      const chunkEnd = Number(chunk.end || chunkStart + 0.2);
      layout.lines.forEach((line) => {
        pushStyledText({
          text: line.text,
          start: chunkStart,
          end: chunkEnd,
          x: line.x,
          y: line.y,
          fontSize: layout.fontSize,
          color: style.textColor
        });

        line.tokens.forEach((token) => {
          const activeFontSize = style.activeWordPop
            ? Math.max(12, Math.round(layout.fontSize * Number(style.activeWordScale || 1.25)))
            : layout.fontSize;
          const activeTokenWidth = estimateTextWidth(token.word, activeFontSize);
          const x = Number(token.x || 0) - Math.max(0, (activeTokenWidth - Number(token.width || 0)) / 2);
          const y = Number(token.y || 0) - Math.max(0, (activeFontSize - layout.fontSize) / 2);
          pushStyledText({
            text: token.word,
            start: token.start,
            end: token.end,
            x,
            y,
            fontSize: activeFontSize,
            color: style.highlightColor
          });
        });
      });
    });

    if (!filters.length) return null;
    return {
      expr: filters.join(';'),
      lastLabel: label
    };
  };

  const downloadsDir = app.getPath('downloads');
  const outputSafeBase =
    outputNameRaw.replace(/[^\w.-]+/g, '-').replace(/\.mp4$/i, '') || `tiktok-subtitles-${Date.now()}`;
  const outputPath = path.join(downloadsDir, `${outputSafeBase}.mp4`);
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'tiktok-caption-'));
  let introOverlayPrepared = introOverlay;

  const runFfmpeg = (args) =>
    new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', sanitizeFfmpegArgs(args), { stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      let maxSentPercent = 0;
      const parseFfmpegTimestampSec = (line = '') => {
        const match = String(line).match(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/);
        if (!match) return null;
        const h = Number(match[1] || 0);
        const m = Number(match[2] || 0);
        const s = Number(match[3] || 0);
        if (![h, m, s].every(Number.isFinite)) return null;
        return h * 3600 + m * 60 + s;
      };
      proc.stderr.on('data', (data) => {
        const text = data.toString();
        err += text;
        if (progressDurationSec > 0) {
          const lines = text.split(/[\r\n]+/);
          lines.forEach((line) => {
            const timeSec = parseFfmpegTimestampSec(line);
            if (!Number.isFinite(timeSec)) return;
            const percent = Math.max(0, Math.min(99, Math.round((timeSec / progressDurationSec) * 100)));
            if (percent > maxSentPercent) {
              maxSentPercent = percent;
              sendCaptionRenderProgress(percent, 'Rendering captions');
            }
          });
        }
      });
      proc.on('error', (error) => reject(error));
      proc.on('close', (code) => {
        if (code === 0) {
          sendCaptionRenderProgress(100, 'Completed');
          resolve();
        }
        else reject(new Error(err || `ffmpeg exited with ${code}`));
      });
    });

  try {
    if (introOverlayPrepared && /^https?:\/\//i.test(String(introOverlayPrepared.url || ''))) {
      sendCaptionRenderProgress(2, 'Caching remote intro source');
      const extMatch = String(introOverlayPrepared.url).match(/\.([a-z0-9]{2,6})(?:\?|$)/i);
      const ext = extMatch ? `.${extMatch[1].toLowerCase()}` : '.mp4';
      const cachedIntroPath = path.join(tmpRoot, `intro-remote-${Date.now()}${ext}`);
      await downloadRemoteMediaToFile(introOverlayPrepared.url, cachedIntroPath);
      introOverlayPrepared = {
        ...introOverlayPrepared,
        url: cachedIntroPath
      };
      sendCaptionRenderProgress(3, 'Remote intro source cached');
    }
    if (
      introOverlayPrepared &&
      !introOverlayPrepared.isImage &&
      !!introOverlayPrepared.detectForeground &&
      !!String(introOverlayPrepared.url || '').trim()
    ) {
      sendCaptionRenderProgress(3, 'Extracting avatar foreground for intro mask');
      const extractedPath = path.join(tmpRoot, `intro-foreground-${Date.now()}.mp4`);
      const maskPath = path.join(tmpRoot, `intro-foreground-mask-${Date.now()}.mp4`);
      const extraction = await runAvatarForegroundExtraction({
        inputPath: introOverlayPrepared.url,
        outputPath: extractedPath,
        maskOutputPath: maskPath,
        startSec: introOverlayPrepared.startSec,
        endSec: introOverlayPrepared.endSec
      });
      const clippedDuration = Math.max(0.2, Number(introOverlayPrepared.endSec || 0) - Number(introOverlayPrepared.startSec || 0));
      introOverlayPrepared = {
        ...introOverlayPrepared,
        url: extraction.colorPath,
        maskPath: extraction.maskPath || '',
        startSec: 0,
        endSec: clippedDuration,
        mode: 'alphamask',
        fullFrame: true
      };
      if (!introOverlayPrepared.maskPath) {
        throw new Error('Foreground extraction did not produce a usable alpha mask for intro overlay.');
      }
      sendCaptionRenderProgress(6, 'Foreground extraction complete');
    }
    if (!renderPlanOps.length) {
      return failCaptionRender('Render plan is missing. Regenerate transcript and try again.', {
        includePipelineVersion: true
      });
    }
    const drawtextFilterPreferred = buildDrawtextFilterFromPlan({ includeFont: true });
    const drawtextFilterFallback = buildDrawtextFilterFromPlan({ includeFont: false });
    const filterCandidates = [drawtextFilterPreferred, drawtextFilterFallback].filter(
      (entry) => entry && typeof entry.expr === 'string' && entry.expr.trim()
    );
    if (!filterCandidates.length) {
      return failCaptionRender('Drawtext filter could not be built from the live preview plan.', {
        includePipelineVersion: true
      });
    }
    const buildEncoderArgs = (encoder = '') => {
      if (encoder === 'libx264') {
        return ['-c:v', 'libx264', '-crf', '20'];
      }
      if (encoder === 'libopenh264') {
        return ['-c:v', 'libopenh264', '-b:v', '4M'];
      }
      if (encoder === 'mpeg4') {
        return ['-c:v', 'mpeg4', '-q:v', '4'];
      }
      return ['-c:v', 'libx264', '-crf', '20'];
    };
    const writeFilterScript = (filterSpec = null, { useIntroOverlay = false } = {}) => {
      const scriptPath = path.join(tmpRoot, `filter-${Date.now()}-${Math.random().toString(36).slice(2)}.txt`);
      const filterExpr = String(filterSpec?.expr || '');
      const maxLabel = Math.max(0, Number(filterSpec?.lastLabel || 0));
      const safeStart = Number(introOverlayPrepared?.startSec || 0);
      const safeEnd = Number(introOverlayPrepared?.endSec || safeStart + 0.8);
      const shouldOverlayIntro = !!(useIntroOverlay && introOverlayPrepared && introOverlayPrepared.url);
      const chain = shouldOverlayIntro
        ? (() => {
            if (introOverlayPrepared.fullFrame && introOverlayPrepared.maskPath) {
              return (
                `[0:v]null[v0];${filterExpr};` +
                `[1:v]trim=start=${safeStart.toFixed(3)}:end=${safeEnd.toFixed(3)},setpts=PTS-STARTPTS,format=rgba[intro_color];` +
                `[2:v]trim=start=${safeStart.toFixed(3)}:end=${safeEnd.toFixed(3)},setpts=PTS-STARTPTS,format=gray[intro_mask];` +
                `[intro_color][intro_mask]alphamerge[intro_ovl];` +
                `[v${maxLabel}][intro_ovl]overlay=0:0:format=auto:enable='between(t,${safeStart.toFixed(3)},${safeEnd.toFixed(3)})'[vout]`
              );
            }
            const overlayW = Math.max(80, Math.round(width * Number(introOverlayPrepared.scale || 0.52)));
            const xCenter = Math.max(0, Math.min(100, Number(introOverlayPrepared.centerXPercent || 50)));
            const marginBottomPx = Math.max(0, Math.round(height * Number(introOverlayPrepared.marginBottomPercent || 0.03)));
            const overlayXExpr = `(W*${(xCenter / 100).toFixed(4)})-overlay_w/2`;
            const overlayYExpr = `${Math.max(0, Math.round(height))}-overlay_h-${marginBottomPx}`;
            const keyFilter =
              introOverlayPrepared.mode === 'greenscreen'
                ? `colorkey=0x00FF00:${Number(introOverlayPrepared.greenSimilarity || 0.24).toFixed(3)}:${Number(
                    introOverlayPrepared.greenBlend || 0.06
                  ).toFixed(3)}`
                : `colorkey=0x00FF00:${Number(introOverlayPrepared.greenSimilarity || 0.24).toFixed(3)}:${Number(
                    introOverlayPrepared.greenBlend || 0.06
                  ).toFixed(3)},colorkey=0x000000:${Number(introOverlayPrepared.blackSimilarity || 0.04).toFixed(3)}:${Number(
                    introOverlayPrepared.blackBlend || 0.01
                  ).toFixed(3)}`;
            return (
              `[0:v]null[v0];${filterExpr};` +
              `[1:v]scale=${overlayW}:-1,format=rgba,${keyFilter}[intro_ovl];` +
              `[v${maxLabel}][intro_ovl]overlay=${overlayXExpr}:${overlayYExpr}:format=auto:enable='between(t,${safeStart.toFixed(
                3
              )},${safeEnd.toFixed(3)})'[vout]`
            );
          })()
        : `[0:v]null[v0];${filterExpr};[v${maxLabel}]null[vout]`;
      fs.writeFileSync(scriptPath, chain, 'utf8');
      return scriptPath;
    };

    const tryEncoders = async ({ audioMode = 'copy' } = {}) => {
      const encoders = ['libx264', 'libopenh264', 'mpeg4'];
      let lastErr = null;
      let introOverlayApplied = false;
      const overlayAttempts = introOverlayPrepared ? [true, false] : [false];
      for (const filterSpec of filterCandidates) {
        for (const useIntroOverlay of overlayAttempts) {
          const filterScript = writeFilterScript(filterSpec, { useIntroOverlay });
          for (const enc of encoders) {
            try {
              const args = ['-i', videoPath];
              if (useIntroOverlay && introOverlayPrepared?.url) {
                if (introOverlayPrepared.isImage) args.push('-loop', '1', '-i', introOverlayPrepared.url);
                else args.push('-stream_loop', '-1', '-i', introOverlayPrepared.url);
                if (introOverlayPrepared.fullFrame && introOverlayPrepared.maskPath) {
                  args.push('-stream_loop', '-1', '-i', introOverlayPrepared.maskPath);
                }
              }
              args.push(
                '-filter_complex_script',
                filterScript,
                '-map',
                '[vout]',
                '-map',
                '0:a?',
                ...buildEncoderArgs(enc),
                '-pix_fmt',
                'yuv420p'
              );
              if (audioMode === 'copy') {
                args.push('-c:a', 'copy');
              } else {
                args.push('-c:a', 'aac', '-b:a', '192k');
              }
              args.push('-movflags', '+faststart', '-y', outputPath);
              await runFfmpeg(args);
              introOverlayApplied = !!useIntroOverlay;
              return { introOverlayApplied };
            } catch (err) {
              lastErr = err;
            }
          }
        }
      }
      if (lastErr) throw lastErr;
      throw new Error('No compatible encoder available for caption render.');
    };
    let introOverlayApplied = false;
    try {
      const copyResult = await tryEncoders({ audioMode: 'copy' });
      introOverlayApplied = !!copyResult?.introOverlayApplied;
    } catch (_err) {
      const aacResult = await tryEncoders({ audioMode: 'aac' });
      introOverlayApplied = !!aacResult?.introOverlayApplied;
    }
    return {
      success: true,
      path: outputPath,
      words: words.length,
      overlayStyle: style,
      introOverlayApplied,
      pipelineVersion: CAPTION_RENDER_PIPELINE_VERSION
    };
  } catch (err) {
    return failCaptionRender(err.message || 'Subtitle render failed.', {
      includePipelineVersion: true
    });
  } finally {
    try {
      fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch (_err) {}
  }
});

ipcMain.handle('combine-tiktok-caption-intro-mask', async (_event, payload = {}) => {
  const failCombine = (message, { includePipelineVersion = false } = {}) => {
    const base = String(message || 'Step 9 combine failed.').trim() || 'Step 9 combine failed.';
    const full = includePipelineVersion ? `[${CAPTION_RENDER_PIPELINE_VERSION}] ${base}` : base;
    return {
      success: false,
      error: full,
      errorCopied: copyTextToClipboardSafe(full)
    };
  };

  const parseFileUrlPath = (value = '') => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    if (!/^file:\/\//i.test(raw)) return raw;
    try {
      return decodeURIComponent(new URL(raw).pathname || '');
    } catch (_err) {
      return raw;
    }
  };

  const videoPath = parseFileUrlPath(payload.videoPath);
  const overlayColorPath = parseFileUrlPath(payload.overlayColorPath || payload.colorPath || '');
  const overlayMaskPath = parseFileUrlPath(payload.overlayMaskPath || payload.maskPath || '');
  if (!videoPath) return failCombine('No caption video path provided.');
  if (!overlayColorPath || !overlayMaskPath) return failCombine('Masked overlay inputs are missing.');
  if (!fs.existsSync(videoPath)) return failCombine('Caption video not found.');
  if (!fs.existsSync(overlayColorPath)) return failCombine('Masked color video not found.');
  if (!fs.existsSync(overlayMaskPath)) return failCombine('Masked alpha mask video not found.');

  const startSec = Math.max(0, Number(payload.startSec || 0));
  const endSec = Math.max(startSec + 0.08, Number(payload.endSec || startSec + 0.8));
  const outputNameRaw = String(payload.outputName || '').trim();
  const progressRunId = String(payload.progressRunId || '').trim();
  const sendCombineProgress = (percent = 0, message = '') => {
    if (!progressRunId) return;
    const webContents = _event?.sender;
    if (!webContents || webContents.isDestroyed()) return;
    try {
      webContents.send('tiktok-caption-render-progress', {
        runId: progressRunId,
        percent: Math.max(0, Math.min(100, Number(percent) || 0)),
        message: String(message || '')
      });
    } catch (_err) {}
  };

  const probeDurationSec = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve(0);
        return;
      }
      let out = '';
      const proc = spawn(
        'ffprobe',
        ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', targetPath],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve(0));
      proc.on('close', () => {
        const value = Number(String(out || '').trim().split('\n').pop() || 0);
        resolve(Number.isFinite(value) && value > 0 ? value : 0);
      });
    });
  const probeResolution = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve({ width: 0, height: 0 });
        return;
      }
      let out = '';
      const proc = spawn(
        'ffprobe',
        [
          '-v',
          'error',
          '-select_streams',
          'v:0',
          '-show_entries',
          'stream=width,height',
          '-of',
          'csv=p=0:s=x',
          targetPath
        ],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve({ width: 0, height: 0 }));
      proc.on('close', () => {
        const raw = String(out || '').trim().split('\n').pop() || '';
        const [wRaw, hRaw] = raw.split('x');
        const width = Number(wRaw || 0);
        const height = Number(hRaw || 0);
        resolve({
          width: Number.isFinite(width) && width > 0 ? Math.round(width) : 0,
          height: Number.isFinite(height) && height > 0 ? Math.round(height) : 0
        });
      });
    });

  const durationSecFromPayload = Number(payload.durationSec || 0);
  const durationSec =
    durationSecFromPayload > 0 ? durationSecFromPayload : await probeDurationSec(videoPath);
  const baseRes = await probeResolution(videoPath);
  const colorRes = await probeResolution(overlayColorPath);
  const maskRes = await probeResolution(overlayMaskPath);
  const baseW = Math.max(2, Number(baseRes.width || 0));
  const baseH = Math.max(2, Number(baseRes.height || 0));
  if (!baseW || !baseH) {
    return failCombine('Could not read caption video dimensions for combine.');
  }

  const downloadsDir = app.getPath('downloads');
  const outputSafeBase =
    outputNameRaw.replace(/[^\w.-]+/g, '-').replace(/\.mp4$/i, '') || `tiktok-step9-combined-${Date.now()}`;
  const outputPath = path.join(downloadsDir, `${outputSafeBase}.mp4`);
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'tiktok-caption-combine-'));

  const buildEncoderArgs = (encoder = '') => {
    if (encoder === 'libx264') {
      return ['-c:v', 'libx264', '-crf', '20'];
    }
    if (encoder === 'libopenh264') {
      return ['-c:v', 'libopenh264', '-b:v', '4M'];
    }
    if (encoder === 'mpeg4') {
      return ['-c:v', 'mpeg4', '-q:v', '4'];
    }
    return ['-c:v', 'libx264', '-crf', '20'];
  };

  const writeFilterScript = () => {
    const scriptPath = path.join(tmpRoot, `combine-filter-${Date.now()}-${Math.random().toString(36).slice(2)}.txt`);
    const chain =
      `[0:v]format=rgba[base];` +
      `[1:v]trim=start=${startSec.toFixed(3)}:end=${endSec.toFixed(3)},setpts=PTS-STARTPTS,format=rgba,scale=${baseW}:${baseH}:flags=lanczos[intro_color];` +
      `[2:v]trim=start=${startSec.toFixed(3)}:end=${endSec.toFixed(3)},setpts=PTS-STARTPTS,format=gray,scale=${baseW}:${baseH}:flags=neighbor[intro_mask];` +
      `[intro_color][intro_mask]alphamerge[intro_ovl];` +
      `[base][intro_ovl]overlay=0:0:format=auto:enable='between(t,${startSec.toFixed(3)},${endSec.toFixed(3)})'[vout]`;
    fs.writeFileSync(scriptPath, chain, 'utf8');
    return scriptPath;
  };

  const runFfmpeg = (args) =>
    new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', sanitizeFfmpegArgs(args), { stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      let maxSentPercent = 0;
      proc.stderr.on('data', (data) => {
        const text = String(data || '');
        err += text;
        if (durationSec > 0) {
          const lines = text.split(/[\r\n]+/);
          lines.forEach((line) => {
            const match = String(line).match(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/);
            if (!match) return;
            const h = Number(match[1] || 0);
            const m = Number(match[2] || 0);
            const s = Number(match[3] || 0);
            if (![h, m, s].every(Number.isFinite)) return;
            const timeSec = h * 3600 + m * 60 + s;
            const percent = Math.max(0, Math.min(99, Math.round((timeSec / durationSec) * 100)));
            if (percent > maxSentPercent) {
              maxSentPercent = percent;
              sendCombineProgress(percent, 'Combining caption + intro mask');
            }
          });
        }
      });
      proc.on('error', (error) => reject(error));
      proc.on('close', (code) => {
        if (code === 0) {
          sendCombineProgress(100, 'Completed');
          resolve();
        } else {
          reject(new Error(err || `ffmpeg exited with ${code}`));
        }
      });
    });

  try {
    sendCombineProgress(2, 'Preparing combine pipeline');
    sendCombineProgress(
      3,
      `Dimensions base=${baseW}x${baseH} color=${Math.max(0, Number(colorRes.width || 0))}x${Math.max(
        0,
        Number(colorRes.height || 0)
      )} mask=${Math.max(0, Number(maskRes.width || 0))}x${Math.max(0, Number(maskRes.height || 0))}`
    );
    const filterScript = writeFilterScript();
    const encoders = ['libx264', 'libopenh264', 'mpeg4'];
    const tryEncoders = async ({ audioMode = 'copy' } = {}) => {
      let lastErr = null;
      for (const enc of encoders) {
        try {
          const args = [
            '-i',
            videoPath,
            '-stream_loop',
            '-1',
            '-i',
            overlayColorPath,
            '-stream_loop',
            '-1',
            '-i',
            overlayMaskPath,
            '-filter_complex_script',
            filterScript,
            '-map',
            '[vout]',
            '-map',
            '0:a?',
            ...buildEncoderArgs(enc),
            '-pix_fmt',
            'yuv420p'
          ];
          if (audioMode === 'copy') args.push('-c:a', 'copy');
          else args.push('-c:a', 'aac', '-b:a', '192k');
          args.push('-movflags', '+faststart', '-y', outputPath);
          sendCombineProgress(6, `Encoding with ${enc}`);
          await runFfmpeg(args);
          return;
        } catch (err) {
          lastErr = err;
        }
      }
      throw lastErr || new Error('No compatible encoder available for combine pass.');
    };
    try {
      await tryEncoders({ audioMode: 'copy' });
    } catch (_err) {
      await tryEncoders({ audioMode: 'aac' });
    }
    return {
      success: true,
      path: outputPath,
      pipelineVersion: CAPTION_RENDER_PIPELINE_VERSION
    };
  } catch (err) {
    return failCombine(err?.message || 'Step 9 combine failed.', { includePipelineVersion: true });
  } finally {
    try {
      fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch (_err) {}
  }
});

ipcMain.handle('render-tiktok-intro-zoom-video', async (_event, payload = {}) => {
  const failZoom = (message, { includePipelineVersion = false } = {}) => {
    const base = String(message || 'Step 10 zoom render failed.').trim() || 'Step 10 zoom render failed.';
    const full = includePipelineVersion ? `[${CAPTION_RENDER_PIPELINE_VERSION}] ${base}` : base;
    return {
      success: false,
      error: full,
      errorCopied: copyTextToClipboardSafe(full)
    };
  };

  const parseFileUrlPath = (value = '') => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    if (!/^file:\/\//i.test(raw)) return raw;
    try {
      return decodeURIComponent(new URL(raw).pathname || '');
    } catch (_err) {
      return raw;
    }
  };

  const videoPath = parseFileUrlPath(payload.videoPath || '');
  if (!videoPath) return failZoom('No Step 9 input video provided.');
  if (!fs.existsSync(videoPath)) return failZoom('Step 9 input video was not found on disk.');

  const settings = payload.settings && typeof payload.settings === 'object' ? payload.settings : {};
  const enabled = settings.enabled !== false;
  const durationSecRaw = Number(settings.durationSec || 1.2);
  const startScaleRaw = Number(settings.startScale || 1.0);
  const endScaleRaw = Number(settings.endScale || 1.14);
  const easingRaw = String(settings.easing || 'easeOut').trim().toLowerCase();
  const easing = easingRaw === 'linear' ? 'linear' : 'easeOut';

  const durationSec = Math.max(0.3, Math.min(4.0, Number.isFinite(durationSecRaw) ? durationSecRaw : 1.2));
  const startScale = Math.max(1.0, Math.min(1.6, Number.isFinite(startScaleRaw) ? startScaleRaw : 1.0));
  const endScale = Math.max(1.0, Math.min(1.6, Number.isFinite(endScaleRaw) ? endScaleRaw : 1.14));
  const outputNameRaw = String(payload.outputName || '').trim();
  const progressRunId = String(payload.progressRunId || '').trim();

  const sendZoomProgress = (percent = 0, message = '') => {
    if (!progressRunId) return;
    const webContents = _event?.sender;
    if (!webContents || webContents.isDestroyed()) return;
    try {
      webContents.send('tiktok-zoom-render-progress', {
        runId: progressRunId,
        percent: Math.max(0, Math.min(100, Number(percent) || 0)),
        message: String(message || '')
      });
    } catch (_err) {}
  };

  const probeDurationSec = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve(0);
        return;
      }
      let out = '';
      const proc = spawn(
        'ffprobe',
        ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', targetPath],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve(0));
      proc.on('close', () => {
        const value = Number(String(out || '').trim().split('\n').pop() || 0);
        resolve(Number.isFinite(value) && value > 0 ? value : 0);
      });
    });
  const probeResolution = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve({ width: 0, height: 0 });
        return;
      }
      let out = '';
      const proc = spawn(
        'ffprobe',
        [
          '-v',
          'error',
          '-select_streams',
          'v:0',
          '-show_entries',
          'stream=width,height',
          '-of',
          'csv=p=0:s=x',
          targetPath
        ],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve({ width: 0, height: 0 }));
      proc.on('close', () => {
        const raw = String(out || '').trim().split('\n').pop() || '';
        const [wRaw, hRaw] = raw.split('x');
        const width = Number(wRaw || 0);
        const height = Number(hRaw || 0);
        resolve({
          width: Number.isFinite(width) && width > 0 ? Math.round(width) : 0,
          height: Number.isFinite(height) && height > 0 ? Math.round(height) : 0
        });
      });
    });

  const inputDuration = await probeDurationSec(videoPath);
  const inputRes = await probeResolution(videoPath);
  const outW = Math.max(320, Number(inputRes.width || 0) || 1080);
  const outH = Math.max(320, Number(inputRes.height || 0) || 1920);
  const zoomDuration = inputDuration > 0 ? Math.min(durationSec, inputDuration) : durationSec;
  const safeDurationForExpr = Math.max(0.3, zoomDuration);

  const outputDir = path.dirname(videoPath);
  const outputName =
    outputNameRaw.replace(/[^\w.-]+/g, '-').replace(/\.mp4$/i, '') || `tiktok-step10-zoom-${Date.now()}`;
  const outputPath = path.join(outputDir, `${outputName}.mp4`);

  const buildEncoderArgs = (encoder = '') => {
    if (encoder === 'libopenh264') return ['-c:v', 'libopenh264', '-b:v', '8M'];
    if (encoder === 'mpeg4') return ['-c:v', 'mpeg4', '-q:v', '2'];
    return ['-c:v', 'libx264', '-crf', '20'];
  };

  const runFfmpeg = (args) =>
    new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', sanitizeFfmpegArgs(args), { stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      let maxSentPercent = 0;
      proc.stderr.on('data', (data) => {
        const text = String(data || '');
        err += text;
        if (inputDuration > 0) {
          const lines = text.split(/[\r\n]+/);
          lines.forEach((line) => {
            const match = String(line).match(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/);
            if (!match) return;
            const h = Number(match[1] || 0);
            const m = Number(match[2] || 0);
            const s = Number(match[3] || 0);
            if (![h, m, s].every(Number.isFinite)) return;
            const timeSec = h * 3600 + m * 60 + s;
            const percent = Math.max(0, Math.min(99, Math.round((timeSec / inputDuration) * 100)));
            if (percent > maxSentPercent) {
              maxSentPercent = percent;
              sendZoomProgress(percent, 'Rendering intro zoom');
            }
          });
        }
      });
      proc.on('error', (error) => reject(error));
      proc.on('close', (code) => {
        if (code === 0) {
          sendZoomProgress(100, 'Completed');
          resolve();
        } else {
          reject(new Error(err || `ffmpeg exited with ${code}`));
        }
      });
    });

  try {
    sendZoomProgress(2, 'Preparing zoom pipeline');
    const linearProgressExpr = `if(lte(in_time\\,${safeDurationForExpr.toFixed(3)})\\,in_time/${safeDurationForExpr.toFixed(3)}\\,1)`;
    const easedProgressExpr = easing === 'linear' ? linearProgressExpr : `1-pow(1-${linearProgressExpr}\\,2)`;
    const zoomExpr = `${startScale.toFixed(4)}+(${(endScale - startScale).toFixed(4)})*(${easedProgressExpr})`;
    // Use zoompan to guarantee frame-by-frame center-anchored zoom on constrained ffmpeg builds.
    const vf = enabled
      ? `setpts=PTS-STARTPTS,fps=30,zoompan=z='${zoomExpr}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:s=${outW}x${outH}:fps=30,setsar=1`
      : 'null';

    const encoders = ['libopenh264', 'libx264', 'mpeg4'];
    const tryEncoders = async ({ audioMode = 'copy' } = {}) => {
      let lastErr = null;
      for (const enc of encoders) {
        try {
          const args = ['-i', videoPath, '-vf', vf, ...buildEncoderArgs(enc), '-pix_fmt', 'yuv420p'];
          if (audioMode === 'copy') args.push('-c:a', 'copy');
          else args.push('-c:a', 'aac', '-b:a', '192k');
          args.push('-movflags', '+faststart', '-y', outputPath);
          sendZoomProgress(5, `Encoding with ${enc}`);
          await runFfmpeg(args);
          return;
        } catch (err) {
          lastErr = err;
        }
      }
      throw lastErr || new Error('No compatible encoder available for Step 10 zoom render.');
    };

    try {
      await tryEncoders({ audioMode: 'copy' });
    } catch (_err) {
      await tryEncoders({ audioMode: 'aac' });
    }
    return {
      success: true,
      path: outputPath,
      pipelineVersion: CAPTION_RENDER_PIPELINE_VERSION
    };
  } catch (err) {
    return failZoom(err?.message || 'Step 10 zoom render failed.', { includePipelineVersion: true });
  }
});

ipcMain.handle('render-tiktok-sfx-video', async (_event, payload = {}) => {
  const failSfx = (message, { includePipelineVersion = false } = {}) => {
    const base = String(message || 'Step 11 SFX render failed.').trim() || 'Step 11 SFX render failed.';
    const full = includePipelineVersion ? `[${CAPTION_RENDER_PIPELINE_VERSION}] ${base}` : base;
    return {
      success: false,
      error: full,
      errorCopied: copyTextToClipboardSafe(full)
    };
  };

  const parseFileUrlPath = (value = '') => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    if (!/^file:\/\//i.test(raw)) return raw;
    try {
      return decodeURIComponent(new URL(raw).pathname || '');
    } catch (_err) {
      return raw;
    }
  };
  const clampNumeric = (value, min, max, fallback) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(min, Math.min(max, n));
  };

  const videoPath = parseFileUrlPath(payload.videoPath || '');
  if (!videoPath) return failSfx('No Step 11 input video provided.');
  if (!fs.existsSync(videoPath)) return failSfx('Step 11 input video was not found on disk.');
  const eventsRaw = Array.isArray(payload.events) ? payload.events : [];
  const events = eventsRaw
    .map((event, idx) => {
      const timeSec = Math.max(0, Number(event?.timeSec ?? event?.time ?? 0));
      const typeRaw = String(event?.type || '').trim().toLowerCase();
      const type = typeRaw === 'hit' || typeRaw === 'whoosh' ? typeRaw : 'pop';
      const gain = clampNumeric(event?.gain, 0.05, 1.5, 1);
      const query = String(event?.query || '').trim();
      const label = String(event?.label || '').trim();
      const sourceRaw = String(event?.source || '').trim().toLowerCase();
      const source = sourceRaw === 'local' || sourceRaw === 'dynamic' ? sourceRaw : 'auto';
      const localPathHint = parseFileUrlPath(event?.localPathHint || '') || String(event?.localPathHint || '').trim();
      const localNameHint = String(event?.localNameHint || '').trim();
      return {
        id: String(event?.id || `sfx-${idx + 1}`),
        timeSec,
        type,
        gain,
        query,
        label,
        source,
        localPathHint,
        localNameHint
      };
    })
    .filter((event) => Number.isFinite(event.timeSec))
    .sort((a, b) => a.timeSec - b.timeSec);
  if (!events.length) return failSfx('No Step 11 SFX events were provided.');

  const styleRaw = String(payload.style || 'engaging').trim().toLowerCase();
  const style = styleRaw === 'clean' || styleRaw === 'hype' ? styleRaw : 'engaging';
  const intensity = clampNumeric(payload.intensity, 0, 100, 60);
  const volume = clampNumeric(payload.volume, 0, 5000, 100);
  const outputNameRaw = String(payload.outputName || '').trim();
  const progressRunId = String(payload.progressRunId || '').trim();

  const progressWebContents = _event?.sender;
  const progressFrame = _event?.senderFrame;
  let progressChannelClosed = false;
  const closeProgressChannel = () => {
    progressChannelClosed = true;
  };
  if (progressWebContents && typeof progressWebContents.once === 'function') {
    progressWebContents.once('destroyed', closeProgressChannel);
    progressWebContents.once('render-process-gone', closeProgressChannel);
  }

  let lastSfxPercent = 0;
  const sendSfxProgress = (percent = null, message = '', trace = null) => {
    if (!progressRunId || progressChannelClosed) return;
    const webContents = progressWebContents;
    if (!webContents || webContents.isDestroyed()) {
      progressChannelClosed = true;
      return;
    }
    if (typeof webContents.isCrashed === 'function' && webContents.isCrashed()) {
      progressChannelClosed = true;
      return;
    }
    if (progressFrame && typeof progressFrame.isDestroyed === 'function' && progressFrame.isDestroyed()) {
      progressChannelClosed = true;
      return;
    }
    try {
      if (Number.isFinite(Number(percent))) {
        lastSfxPercent = Math.max(0, Math.min(100, Number(percent)));
      }
      const payload = {
        runId: progressRunId,
        percent: lastSfxPercent,
        message: String(message || '')
      };
      if (trace && typeof trace === 'object') payload.trace = trace;
      webContents.send('tiktok-sfx-render-progress', {
        ...payload
      });
    } catch (err) {
      if (/render frame was disposed/i.test(String(err?.message || ''))) {
        progressChannelClosed = true;
      }
    }
  };

  const probeDurationSec = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve(0);
        return;
      }
      let out = '';
      const proc = spawn(
        'ffprobe',
        ['-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', targetPath],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve(0));
      proc.on('close', () => {
        const value = Number(String(out || '').trim().split('\n').pop() || 0);
        resolve(Number.isFinite(value) && value > 0 ? value : 0);
      });
    });

  const probeHasAudio = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve(false);
        return;
      }
      const proc = spawn(
        'ffprobe',
        ['-v', 'error', '-select_streams', 'a:0', '-show_entries', 'stream=index', '-of', 'csv=p=0', targetPath],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      let out = '';
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve(false));
      proc.on('close', () => resolve(!!String(out || '').trim()));
    });
  const probeIntegratedLufs = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve(null);
        return;
      }
      let err = '';
      const proc = spawn(
        'ffmpeg',
        [
          '-hide_banner',
          '-nostats',
          '-i',
          targetPath,
          '-vn',
          '-af',
          'loudnorm=I=-16:TP=-1.5:LRA=11:print_format=json',
          '-f',
          'null',
          '-'
        ],
        { stdio: ['ignore', 'ignore', 'pipe'] }
      );
      proc.stderr.on('data', (chunk) => {
        err += String(chunk || '');
      });
      proc.on('error', () => resolve(null));
      proc.on('close', () => {
        const match = String(err || '').match(/\{\s*"input_i"[\s\S]*?\}/);
        if (!match) {
          resolve(null);
          return;
        }
        try {
          const parsed = JSON.parse(match[0]);
          const value = Number(parsed?.input_i);
          resolve(Number.isFinite(value) ? value : null);
        } catch (_err) {
          resolve(null);
        }
      });
    });

  const inputDuration = await probeDurationSec(videoPath);
  const hasAudio = await probeHasAudio(videoPath);
  const baseInputLufs = hasAudio ? await probeIntegratedLufs(videoPath) : null;
  const outputDir = path.dirname(videoPath);
  const outputName =
    outputNameRaw.replace(/[^\w.-]+/g, '-').replace(/\.mp4$/i, '') || `tiktok-step11-sfx-${Date.now()}`;
  const outputPath = path.join(outputDir, `${outputName}.mp4`);
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'tiktok-sfx-'));
  const freesoundToken = String(payload.freesoundToken || payload.freesoundApiKey || '').trim();
  const localLibrary = (Array.isArray(payload.localLibrary) ? payload.localLibrary : [])
    .map((item, idx) => {
      const localPath = parseFileUrlPath(item?.path || '');
      if (!localPath) return null;
      const name = String(item?.name || path.basename(localPath)).trim() || `local-${idx + 1}`;
      const keywords = Array.isArray(item?.keywords)
        ? item.keywords.map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
        : String(item?.keywords || '')
            .toLowerCase()
            .split(/\s|,/)
            .map((k) => k.trim())
            .filter(Boolean);
      return {
        id: String(item?.id || `local-${idx + 1}`),
        path: localPath,
        name,
        keywords: Array.from(new Set(keywords)).slice(0, 16)
      };
    })
    .filter((item) => item && item.path && fs.existsSync(item.path));

  const styleScale = style === 'clean' ? 0.82 : style === 'hype' ? 1.15 : 1;
  const intensityScale = 0.55 + intensity / 100;
  const volumeScale = Math.max(0, volume / 100);
  const normalizeSearchQuery = (value = '') =>
    String(value || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .slice(0, 120);
  const tokenizeSimple = (value = '') =>
    normalizeSearchQuery(value)
      .replace(/[^a-z0-9+\-\s]/g, ' ')
      .split(' ')
      .map((w) => w.trim())
      .filter(Boolean);
  const GENERIC_QUERY_WORDS = new Set([
    'sound',
    'sounds',
    'sfx',
    'effect',
    'effects',
    'audio',
    'noise',
    'content',
    'excitement'
  ]);
  const MOOD_QUERY_WORDS = new Set([
    'curious',
    'mysterious',
    'playful',
    'dramatic',
    'emotional',
    'uplifting',
    'tense',
    'suspenseful',
    'warm',
    'sad',
    'happy',
    'calm'
  ]);
  const SOURCE_HINT_WORDS = new Set([
    'music',
    'ambience',
    'ambient',
    'meow',
    'purr',
    'purring',
    'trill',
    'chirp',
    'hiss',
    'voice',
    'vocal',
    'whoosh',
    'swoosh',
    'pop',
    'click',
    'ding',
    'chime',
    'buzzer',
    'heartbeat'
  ]);
  const TONE_EXEMPT_WORDS = new Set([
    'notification',
    'ringtone',
    'phone',
    'sms',
    'text',
    'alarm',
    'ui',
    'interface',
    'button',
    'click',
    'beep',
    'ding',
    'chime'
  ]);
  const buildQueryCandidates = (event = {}) => {
    const rawQuery = normalizeSearchQuery(event?.query || '');
    const rawLabel = normalizeSearchQuery(event?.label || '');
    const rawReason = normalizeSearchQuery(event?.reason || '');
    const reduceGenericTerms = (value = '') => {
      const words = String(value || '')
        .split(' ')
        .map((w) => w.trim())
        .filter(Boolean);
      const reduced = words.filter((word) => !GENERIC_QUERY_WORDS.has(word));
      return normalizeSearchQuery(reduced.join(' '));
    };
    const normalizeIntentTokens = (value = '') => {
      let tokens = tokenizeSimple(reduceGenericTerms(value));
      if (!tokens.length) return [];
      const contextTokens = tokenizeSimple(`${rawQuery} ${rawLabel} ${rawReason} ${tokens.join(' ')}`);
      if (tokens.includes('tone')) {
        const keepTone = contextTokens.some((tok) => TONE_EXEMPT_WORDS.has(tok));
        if (!keepTone) tokens = tokens.map((tok) => (tok === 'tone' ? 'music' : tok));
      }
      const hasMood = tokens.some((tok) => MOOD_QUERY_WORDS.has(tok));
      const hasSource = tokens.some((tok) => SOURCE_HINT_WORDS.has(tok));
      if (hasMood && !hasSource) tokens.push('music');
      if (tokens.includes('cat') && tokens.includes('greeting') && !tokens.some((tok) => ['meow', 'purr', 'purring', 'trill', 'chirp', 'hiss'].includes(tok))) {
        tokens.push('meow');
      }
      tokens = Array.from(new Set(tokens.filter((tok) => !GENERIC_QUERY_WORDS.has(tok))));
      return tokens.slice(0, 4);
    };
    const out = [];
    const pushQuery = (tokens = []) => {
      const clean = normalizeSearchQuery((Array.isArray(tokens) ? tokens : []).join(' '));
      if (!clean || out.includes(clean)) return;
      out.push(clean);
    };
    const sourceValues = [rawQuery, rawLabel, rawReason].filter(Boolean);
    sourceValues.forEach((value) => {
      const tokens = normalizeIntentTokens(value);
      if (!tokens.length) return;
      pushQuery(tokens);
      const hasMood = tokens.some((tok) => MOOD_QUERY_WORDS.has(tok));
      const hasSource = tokens.some((tok) => SOURCE_HINT_WORDS.has(tok));
      if (hasMood && !hasSource) {
        const baseMood = tokens.filter((tok) => tok !== 'music' && tok !== 'ambience' && tok !== 'ambient').slice(0, 3);
        pushQuery([...baseMood, 'music']);
        pushQuery([...baseMood, 'ambience']);
      }
      if (tokens.includes('cat') && tokens.includes('greeting')) {
        pushQuery(['cat', 'greeting', 'meow']);
      }
    });
    return out.slice(0, 8);
  };
  const shuffleIndices = (len = 0) => {
    const arr = Array.from({ length: Math.max(0, Number(len) || 0) }, (_v, i) => i);
    for (let i = arr.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      const tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
    return arr;
  };
  const SFX_LOCAL_CATEGORY_MATCHERS = {
    swoosh: ['swoosh', 'whoosh', 'swish', 'swipe', 'riser', 'transition'],
    pop: ['pop', 'plop', 'bubble', 'scenepop'],
    click: ['click', 'mouse', 'tap', 'tick', 'type', 'typing', 'scroll', 'shutter', 'camera'],
    right: ['right', 'correct', 'success', 'win', 'ding'],
    wrong: ['wrong', 'incorrect', 'fail', 'error', 'buzzer', 'boo'],
    magic: ['magic', 'reveal', 'sparkle', 'wizard', 'poof']
  };
  const getCategoryFromEvent = (event = {}) => {
    const signal = `${String(event?.query || '')} ${String(event?.label || '')}`.toLowerCase();
    if (!signal) return '';
    const hit = Object.entries(SFX_LOCAL_CATEGORY_MATCHERS).find(([, patterns]) =>
      patterns.some((pattern) => signal.includes(pattern))
    );
    return hit ? hit[0] : '';
  };
  const buildLocalCategoryPools = (library = []) => {
    const pools = {
      swoosh: [],
      pop: [],
      click: [],
      right: [],
      wrong: [],
      magic: []
    };
    (Array.isArray(library) ? library : []).forEach((item) => {
      const haystack = `${String(item?.name || '')} ${
        Array.isArray(item?.keywords) ? item.keywords.join(' ') : String(item?.keywords || '')
      }`
        .toLowerCase()
        .trim();
      if (!haystack) return;
      Object.entries(SFX_LOCAL_CATEGORY_MATCHERS).forEach(([category, patterns]) => {
        if (patterns.some((pattern) => haystack.includes(pattern))) pools[category].push(item);
      });
    });
    return pools;
  };
  const localCategoryPools = buildLocalCategoryPools(localLibrary);
  const localCategoryBags = {};
  let localLastPickedPath = '';
  const pickFromPool = (pool = [], key = '') => {
    if (!Array.isArray(pool) || !pool.length) return null;
    const bag = localCategoryBags[key];
    const hasValidBag = Array.isArray(bag) && bag.every((idx) => Number.isInteger(idx) && idx >= 0 && idx < pool.length);
    if (!hasValidBag) {
      localCategoryBags[key] = shuffleIndices(pool.length);
    }
    if (!localCategoryBags[key].length) {
      localCategoryBags[key] = shuffleIndices(pool.length);
    }
    const idx = Number(localCategoryBags[key].pop());
    if (!Number.isFinite(idx) || idx < 0 || idx >= pool.length) return pool[0] || null;
    return pool[idx] || null;
  };
  const resolveLocalSampleForEvent = (event = {}) => {
    if (!localLibrary.length) return '';
    const directPath = parseFileUrlPath(event?.localPathHint || '') || String(event?.localPathHint || '').trim();
    if (directPath && fs.existsSync(directPath)) return directPath;
    const nameHint = String(event?.localNameHint || '').trim().toLowerCase();
    if (nameHint) {
      const exactByName = localLibrary.find((item) => String(item?.name || '').trim().toLowerCase() === nameHint);
      if (exactByName?.path && fs.existsSync(exactByName.path)) return exactByName.path;
    }
    const category = getCategoryFromEvent(event);
    if (category && Array.isArray(localCategoryPools[category]) && localCategoryPools[category].length) {
      const first = pickFromPool(localCategoryPools[category], category);
      if (first?.path && fs.existsSync(first.path)) {
        if (first.path === localLastPickedPath && localCategoryPools[category].length > 1) {
          const second = pickFromPool(localCategoryPools[category], category);
          if (second?.path && fs.existsSync(second.path)) {
            localLastPickedPath = second.path;
            return second.path;
          }
        }
        localLastPickedPath = first.path;
        return first.path;
      }
    }
    // If a category is detected but no category pool is available, continue to
    // broader local matching instead of dropping the cue entirely.
    const candidates = buildQueryCandidates(event);
    const queryTokens = Array.from(new Set(candidates.flatMap((q) => tokenizeSimple(q))));
    if (!queryTokens.length) return '';
    const scored = [];
    localLibrary.forEach((item) => {
      const nameTokens = tokenizeSimple(item.name || '');
      const keywordTokens = Array.isArray(item.keywords) ? item.keywords : [];
      const tokenSet = new Set([...nameTokens, ...keywordTokens]);
      let score = 0;
      queryTokens.forEach((tok) => {
        if (tokenSet.has(tok)) score += 2;
        else if ([...tokenSet].some((cand) => cand.startsWith(tok) || tok.startsWith(cand))) score += 1;
      });
      if (!score) return;
      scored.push({ score, item });
    });
    if (!scored.length) return '';
    const maxScore = Math.max(...scored.map((entry) => Number(entry.score || 0)));
    const finalists = scored.filter((entry) => Number(entry.score || 0) >= maxScore);
    const diversified = finalists.filter((entry) => String(entry?.item?.path || '') !== String(localLastPickedPath || ''));
    const pool = diversified.length ? diversified : finalists;
    const picked = pool[Math.floor(Math.random() * pool.length)] || scored[0];
    if (picked?.item?.path) localLastPickedPath = String(picked.item.path);
    return picked?.item?.path && fs.existsSync(picked.item.path) ? picked.item.path : '';
  };
  const fetchFreesoundBank = async ({ token, query = '', max = 6, relaxed = false } = {}) => {
    if (!token || !query) return null;
    const endpoint = new URL('https://freesound.org/apiv2/search/text/');
    endpoint.searchParams.set('query', query);
    endpoint.searchParams.set('page_size', String(Math.max(4, Math.min(20, Number(max) || 8))));
    endpoint.searchParams.set(
      'fields',
      'id,name,license,duration,previews'
    );
    endpoint.searchParams.set(
      'filter',
      relaxed ? 'duration:[1 TO 3]' : 'license:"Creative Commons 0" duration:[1 TO 3]'
    );
    endpoint.searchParams.set('sort', 'score');
    const requestUrl = endpoint.toString();
    const res = await fetch(requestUrl, {
      headers: {
        Authorization: `Token ${token}`
      }
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      const msg = String(json?.detail || json?.error || json?.message || '').trim() || `HTTP ${res.status}`;
      throw new Error(`Freesound API error: ${msg}`);
    }
    const rows = (Array.isArray(json?.results) ? json.results : []).filter((row) => {
      const dur = Number(row?.duration || 0);
      return Number.isFinite(dur) && dur >= 1 && dur <= 3;
    });
    const previewUrls = rows
      .map((row) => {
        const preview =
          String(row?.previews?.['preview-hq-mp3'] || '').trim() ||
          String(row?.previews?.['preview-lq-mp3'] || '').trim();
        return preview;
      })
      .filter(Boolean)
      .slice(0, max);
    return {
      query,
      mode: relaxed ? 'relaxed' : 'strict',
      requestUrl,
      statusCode: Number(res.status || 0),
      resultCount: rows.length,
      previewUrls,
      responsePreview: rows.slice(0, 3).map((row) => ({
        id: Number(row?.id || 0),
        name: String(row?.name || '').trim(),
        duration: Number(row?.duration || 0),
        license: String(row?.license || '').trim()
      }))
    };
  };
  const cacheRemoteAudioFile = async (url, idx = 0, hint = 'sfx') => {
    const clean = String(url || '').trim();
    if (!clean) return { path: '', cacheHit: false };
    fs.mkdirSync(TIKTOK_SFX_PREVIEW_CACHE_DIR, { recursive: true });
    const extRaw = path.extname(clean.split('?')[0] || '').toLowerCase();
    const ext = extRaw === '.wav' || extRaw === '.ogg' || extRaw === '.mp3' ? extRaw : '.mp3';
    let hash = 2166136261;
    for (let i = 0; i < clean.length; i += 1) {
      hash ^= clean.charCodeAt(i);
      hash = Math.imul(hash, 16777619) >>> 0;
    }
    const stableName = `${hint}-${hash.toString(16)}${ext}`;
    const localPath = path.join(TIKTOK_SFX_PREVIEW_CACHE_DIR, stableName);
    if (fs.existsSync(localPath)) {
      try {
        const size = Number(fs.statSync(localPath).size || 0);
        if (size > 0) return { path: localPath, cacheHit: true };
      } catch (_err) {}
    }
    const res = await fetch(clean, { redirect: 'follow' });
    if (!res.ok) throw new Error(`Failed to download Freesound preview (${res.status})`);
    const arr = await res.arrayBuffer();
    fs.writeFileSync(localPath, Buffer.from(arr));
    return { path: localPath, cacheHit: false };
  };
  const previewCache = new Map();
  const queryResultCache = new Map();
  const resolveSampleForEvent = async (event = {}, idx = 0, { allowLocal = true } = {}) => {
    const sendTrace = (stage = 'searching', extra = {}) =>
      sendSfxProgress(null, 'Resolving query-matched clips', {
        eventIndex: idx,
        timeSec: Number(event?.timeSec || 0),
        label: String(event?.label || '').trim(),
        query: String(event?.query || '').trim(),
        status: stage,
        ...extra
      });
    sendSfxProgress(null, 'Resolving query-matched clips', {
      eventIndex: idx,
      timeSec: Number(event?.timeSec || 0),
      label: String(event?.label || '').trim(),
      status: 'searching'
    });
    if (allowLocal) {
      const localMatch = resolveLocalSampleForEvent(event, idx);
      if (localMatch) {
        sendTrace('selected-local', {
          selectedClip: path.basename(localMatch),
          outputPath: localMatch,
          source: 'local'
        });
        return { path: localMatch, source: 'local' };
      }
    }
    if (!freesoundToken) {
      sendTrace('failed', {
        source: 'dynamic',
        error: 'No local match and no Freesound token configured.'
      });
      return { path: '', source: '' };
    }
    const candidates = buildQueryCandidates(event);
    sendTrace('queued', {
      candidates,
      message: candidates.length
        ? `queued ${candidates.length} query candidate(s)`
        : 'no query candidate from AI event'
    });
    for (const query of candidates) {
      if (!query) continue;
      const modes = [{ id: 'strict', relaxed: false }, { id: 'relaxed', relaxed: true }];
      for (const mode of modes) {
        const cacheKey = `${mode.id}:${query}`;
        let bankData = queryResultCache.get(cacheKey);
        if (!bankData) {
          sendTrace('requesting', {
            query,
            mode: mode.id,
            message: 'GET /apiv2/search/text',
            requestUrl: `https://freesound.org/apiv2/search/text/?query=${encodeURIComponent(query)}`
          });
          try {
            bankData = await fetchFreesoundBank({ token: freesoundToken, query, max: 10, relaxed: mode.relaxed });
            queryResultCache.set(cacheKey, bankData);
          } catch (err) {
            sendTrace('failed', {
              query,
              mode: mode.id,
              error: String(err?.message || err || 'Unknown Freesound error').trim()
            });
            continue;
          }
        } else {
          sendTrace('cache-hit', {
            query,
            mode: mode.id,
            message: 'using cached search response'
          });
        }
        const previewUrls = Array.isArray(bankData?.previewUrls) ? bankData.previewUrls : [];
        sendTrace('response', {
          query,
          mode: mode.id,
          hits: previewUrls.length,
          requestUrl: String(bankData?.requestUrl || '').trim(),
          statusCode: Number(bankData?.statusCode || 0),
          resultCount: Number(bankData?.resultCount || 0),
          responsePreview: Array.isArray(bankData?.responsePreview) ? bankData.responsePreview : []
        });
        if (!previewUrls.length) continue;
        const selectedUrl = previewUrls[idx % previewUrls.length];
        if (!selectedUrl) continue;
        if (!previewCache.has(selectedUrl)) {
          sendTrace('downloading', {
            query,
            mode: mode.id,
            selectedUrl,
            message: 'downloading preview audio'
          });
          try {
            const local = await cacheRemoteAudioFile(selectedUrl, previewCache.size, 'query');
            const localPath = String(local?.path || '').trim();
            if (!localPath) throw new Error('No local cache path returned for Freesound preview');
            previewCache.set(selectedUrl, localPath);
            sendTrace(local?.cacheHit ? 'cache-hit' : 'downloaded', {
              query,
              mode: mode.id,
              selectedUrl,
              outputPath: localPath,
              selectedClip: path.basename(localPath),
              message: local?.cacheHit ? 'preview loaded from persistent cache' : 'preview downloaded and cached'
            });
          } catch (err) {
            sendTrace('failed', {
              query,
              mode: mode.id,
              selectedUrl,
              error: String(err?.message || err || 'Failed downloading preview clip').trim()
            });
            continue;
          }
        } else {
          sendTrace('cache-hit', {
            query,
            mode: mode.id,
            selectedUrl,
            selectedClip: path.basename(previewCache.get(selectedUrl) || '')
          });
        }
        const localPath = previewCache.get(selectedUrl);
        if (localPath && fs.existsSync(localPath)) {
          sendTrace('selected', {
            query,
            mode: mode.id,
            hits: previewUrls.length,
            selectedUrl,
            selectedClip: path.basename(localPath),
            outputPath: localPath,
            source: 'dynamic'
          });
          return { path: localPath, source: 'dynamic' };
        }
      }
    }
    sendTrace('failed', {
      error: 'No clip matched query candidates'
    });
    return { path: '', source: '' };
  };
  const describeCueShape = (event = {}) => {
    const signal = `${String(event?.query || '')} ${String(event?.label || '')}`.toLowerCase();
    if (/whoosh|swoosh|riser|transition|swipe/.test(signal)) return { dur: 1.35, vol: 0.16 };
    if (/hit|impact|thud|punch|slam|knock|clap/.test(signal)) return { dur: 1.1, vol: 0.2 };
    if (/meow|cat|vocal|voice|animal/.test(signal)) return { dur: 1.5, vol: 0.22 };
    return { dur: 1.2, vol: 0.16 };
  };

  const targetOutputLufs = -16;
  const loudnessDelta = Number.isFinite(baseInputLufs) ? baseInputLufs - targetOutputLufs : 0;
  const loudnessScale = clampNumeric(0.82 - clampNumeric(loudnessDelta / 12, -0.25, 0.25, 0) * 0.6, 0.62, 1.05, 0.82);
  const queuedEvents = events.slice(0, 180);
  const eventsPerSec = queuedEvents.length / Math.max(1, Number(inputDuration || queuedEvents[queuedEvents.length - 1]?.timeSec || 1));
  const densityScale = clampNumeric(1 - Math.max(0, eventsPerSec - 0.35) * 0.55, 0.6, 1, 1);
  const autoSfxGlobalScale = clampNumeric(loudnessScale * densityScale, 0.52, 1.05, 0.82);
  const mixSfxWeight = clampNumeric(0.72 * autoSfxGlobalScale, 0.4, 0.9, 0.65);

  sendSfxProgress(6, 'Resolving Freesound query clips and local fallbacks', {
    status: 'mix-profile',
    targetOutputLufs,
    baseInputLufs: Number.isFinite(baseInputLufs) ? Number(baseInputLufs) : null,
    eventsPerSec: Number.isFinite(eventsPerSec) ? Number(eventsPerSec.toFixed(3)) : 0,
    autoSfxGlobalScale: Number(autoSfxGlobalScale.toFixed(3)),
    mixSfxWeight: Number(mixSfxWeight.toFixed(3))
  });
  const eventDefs = [];
  let dynamicUsed = 0;
  let localFallbackCursor = 0;
  const maxDynamicAuto = Math.min(24, Math.max(6, Math.ceil(queuedEvents.length * 0.75)));
  const maxDynamicHard = Math.min(36, Math.max(maxDynamicAuto, Math.ceil(queuedEvents.length)));
  for (let idx = 0; idx < queuedEvents.length; idx += 1) {
    const event = queuedEvents[idx];
    const base = describeCueShape(event);
    const dur = clampNumeric(base.dur * styleScale, 1.0, 3.0, base.dur);
    const prevGap = idx > 0 ? Math.max(0, Number(event.timeSec || 0) - Number(queuedEvents[idx - 1]?.timeSec || 0)) : Number.MAX_SAFE_INTEGER;
    const nextGap =
      idx + 1 < queuedEvents.length
        ? Math.max(0, Number(queuedEvents[idx + 1]?.timeSec || 0) - Number(event.timeSec || 0))
        : Number.MAX_SAFE_INTEGER;
    const minGap = Math.min(prevGap, nextGap);
    const crowdScale = clampNumeric(minGap / 0.35, 0.55, 1.05, 1);
    const vol = clampNumeric(
      base.vol * intensityScale * volumeScale * clampNumeric(event.gain, 0.3, 1.6, 1) * autoSfxGlobalScale * crowdScale,
      0.0,
      12.0,
      base.vol
    );
    const timeSec = Math.max(0, Math.min(inputDuration > 0 ? Math.max(0, inputDuration - 0.05) : Number.MAX_SAFE_INTEGER, event.timeSec));
    const delayMs = Math.max(0, Math.round(timeSec * 1000));
    const fadeOutStart = Math.max(0.05, dur - 0.08);
    let samplePath = '';
    const sourcePref = String(event?.source || 'auto').trim().toLowerCase();
    const localOnlyRequested = sourcePref === 'local';
    const dynamicBudget = sourcePref === 'dynamic' ? maxDynamicHard : maxDynamicAuto;
    const dynamicFirstRequested = sourcePref === 'dynamic' || sourcePref === 'auto';
    let attemptedDynamic = false;
    if (dynamicFirstRequested && !localOnlyRequested && dynamicUsed < dynamicBudget) {
      attemptedDynamic = true;
      const resolved = await resolveSampleForEvent(event, idx, { allowLocal: false });
      samplePath = String(resolved?.path || '').trim();
      if (samplePath && String(resolved?.source || '') === 'dynamic') dynamicUsed += 1;
    }
    if (!samplePath && localOnlyRequested) {
      samplePath = resolveLocalSampleForEvent(event, idx);
    }
    if (!samplePath && localOnlyRequested) {
      sendSfxProgress(null, 'Resolving query-matched clips', {
        eventIndex: idx,
        timeSec: Number(event?.timeSec || 0),
        label: String(event?.label || '').trim(),
        query: String(event?.query || '').trim(),
        status: 'skipped',
        source: 'local',
        message: 'No local sound matched this cue.'
      });
    }
    if (!samplePath && !localOnlyRequested) {
      samplePath = resolveLocalSampleForEvent(event, idx);
    }
    const allowDynamic = !samplePath && !localOnlyRequested && !attemptedDynamic && dynamicUsed < dynamicBudget;
    if (allowDynamic) {
      const resolved = await resolveSampleForEvent(event, idx, { allowLocal: false });
      samplePath = String(resolved?.path || '').trim();
      if (samplePath && String(resolved?.source || '') === 'dynamic') dynamicUsed += 1;
    }
    if (!samplePath && !localOnlyRequested && !allowDynamic && (attemptedDynamic || dynamicUsed >= dynamicBudget)) {
      sendSfxProgress(null, 'Resolving query-matched clips', {
        eventIndex: idx,
        timeSec: Number(event?.timeSec || 0),
        label: String(event?.label || '').trim(),
        query: String(event?.query || '').trim(),
        status: 'skipped',
        source: 'dynamic',
        message: 'Dynamic query limit reached; using local fallback if available.'
      });
    }
    if (!samplePath && localLibrary.length) {
      const fallback = localLibrary[localFallbackCursor % localLibrary.length] || null;
      localFallbackCursor += 1;
      const fallbackPath = String(fallback?.path || '').trim();
      if (fallbackPath && fs.existsSync(fallbackPath)) {
        samplePath = fallbackPath;
        sendSfxProgress(null, 'Resolving query-matched clips', {
          eventIndex: idx,
          timeSec: Number(event?.timeSec || 0),
          label: String(event?.label || '').trim(),
          query: String(event?.query || '').trim(),
          status: 'selected-local',
          source: 'local',
          selectedClip: path.basename(fallbackPath),
          outputPath: fallbackPath,
          message: 'Fallback local clip used because no exact match was found.'
        });
      }
    }
    if (!samplePath) continue;
    eventDefs.push({ dur, vol, delayMs, fadeOutStart, samplePath });
  }
  if (!eventDefs.length) {
    return failSfx('No SFX clips were resolved. Add local sounds or provide dynamic cues with a Freesound token.');
  }

  const runFfmpeg = (args, { progressStart = 0, progressEnd = 100, progressMessage = 'Rendering SFX mix' } = {}) =>
    new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', sanitizeFfmpegArgs(args), { stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      let maxSentPercent = 0;
      proc.stderr.on('data', (data) => {
        const text = String(data || '');
        err += text;
        if (inputDuration > 0) {
          const lines = text.split(/[\r\n]+/);
          lines.forEach((line) => {
            const match = String(line).match(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/);
            if (!match) return;
            const h = Number(match[1] || 0);
            const m = Number(match[2] || 0);
            const s = Number(match[3] || 0);
            if (![h, m, s].every(Number.isFinite)) return;
            const timeSec = h * 3600 + m * 60 + s;
            const innerPercent = Math.max(0, Math.min(100, Math.round((timeSec / inputDuration) * 100)));
            const mapped = Math.max(
              progressStart,
              Math.min(progressEnd - 1, Math.round(progressStart + ((progressEnd - progressStart) * innerPercent) / 100))
            );
            if (mapped > maxSentPercent) {
              maxSentPercent = mapped;
              sendSfxProgress(mapped, progressMessage);
            }
          });
        }
      });
      proc.on('error', (error) => reject(error));
      proc.on('close', (code) => {
        if (code === 0) {
          if (progressEnd >= 100) sendSfxProgress(100, 'Completed');
          else sendSfxProgress(progressEnd, progressMessage);
          resolve();
        } else {
          reject(new Error(err || `ffmpeg exited with ${code}`));
        }
      });
    });

  try {
    sendSfxProgress(2, 'Preparing event synthesis');
    const filterParts = [];
    const mixInputs = [];
    eventDefs.forEach((event, idx) => {
      const inputIndex = idx + 1;
      filterParts.push(
        `[${inputIndex}:a]aformat=sample_fmts=fltp:sample_rates=48000:channel_layouts=stereo,` +
          `atrim=0:${event.dur.toFixed(3)},asetpts=PTS-STARTPTS,` +
          `afade=t=in:st=0:d=0.01,afade=t=out:st=${event.fadeOutStart.toFixed(3)}:d=0.03,` +
          `volume=${event.vol.toFixed(4)},adelay=${event.delayMs}|${event.delayMs}[sfx${idx}]`
      );
      mixInputs.push(`[sfx${idx}]`);
    });
    filterParts.push(
      `${mixInputs.join('')}amix=inputs=${Math.max(1, mixInputs.length)}:normalize=0,` +
        `highpass=f=40,lowpass=f=12000,` +
        `dynaudnorm=f=120:g=13:m=8:s=5,` +
        `acompressor=threshold=-20dB:ratio=3.5:attack=5:release=70:makeup=3,` +
        `alimiter=limit=0.95[sfxmix]`
    );
    if (hasAudio) {
      filterParts.push(
        `[0:a]aformat=sample_fmts=fltp:sample_rates=48000:channel_layouts=stereo[base];` +
          `[base][sfxmix]amix=inputs=2:weights='1 ${mixSfxWeight.toFixed(3)}':normalize=0,alimiter=limit=0.97[aout]`
      );
    } else {
      filterParts.push('[sfxmix]alimiter=limit=0.97[aout]');
    }
    const filterComplex = filterParts.join(';');
    const filterPath = path.join(tmpRoot, `sfx-filter-${Date.now()}.txt`);
    fs.writeFileSync(filterPath, filterComplex, 'utf8');

    const mixedOutputPath = path.join(tmpRoot, `sfx-mixed-${Date.now()}.mp4`);
    const args = ['-i', videoPath];
    eventDefs.forEach((event) => {
      args.push('-i', event.samplePath);
    });
    args.push(
      '-filter_complex_script',
      filterPath,
      '-map',
      '0:v',
      '-map',
      '[aout]',
      '-c:v',
      'copy',
      '-c:a',
      'aac',
      '-b:a',
      '192k',
      '-movflags',
      '+faststart',
      '-y',
      mixedOutputPath
    );
    sendSfxProgress(8, 'Mixing base audio with SFX layer');
    await runFfmpeg(args, {
      progressStart: 8,
      progressEnd: 92,
      progressMessage: 'Rendering SFX mix'
    });
    sendSfxProgress(93, 'Normalizing final audio loudness');
    await runFfmpeg(
      [
        '-i',
        mixedOutputPath,
        '-map',
        '0:v:0',
        '-map',
        '0:a:0',
        '-c:v',
        'copy',
        '-af',
        `loudnorm=I=${targetOutputLufs}:TP=-1.5:LRA=11`,
        '-c:a',
        'aac',
        '-b:a',
        '192k',
        '-movflags',
        '+faststart',
        '-y',
        outputPath
      ],
      {
        progressStart: 93,
        progressEnd: 99,
        progressMessage: 'Normalizing final audio loudness'
      }
    );
    sendSfxProgress(100, 'Completed');
    return {
      success: true,
      path: outputPath,
      pipelineVersion: CAPTION_RENDER_PIPELINE_VERSION
    };
  } catch (err) {
    return failSfx(err?.message || 'Step 11 SFX render failed.', { includePipelineVersion: true });
  } finally {
    if (progressWebContents && typeof progressWebContents.removeListener === 'function') {
      progressWebContents.removeListener('destroyed', closeProgressChannel);
      progressWebContents.removeListener('render-process-gone', closeProgressChannel);
    }
    try {
      fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch (_err) {}
  }
});

ipcMain.handle('preview-tiktok-intro-mask', async (_event, payload = {}) => {
  const failPreview = (message, { includePipelineVersion = false } = {}) => {
    const base = String(message || 'Intro mask preview failed.').trim() || 'Intro mask preview failed.';
    const full = includePipelineVersion ? `[${CAPTION_RENDER_PIPELINE_VERSION}] ${base}` : base;
    return {
      success: false,
      error: full,
      errorCopied: copyTextToClipboardSafe(full)
    };
  };

  const parseFileUrlPath = (value = '') => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    if (!/^file:\/\//i.test(raw)) return raw;
    try {
      return decodeURIComponent(new URL(raw).pathname || '');
    } catch (_err) {
      return raw;
    }
  };
  const clampNumeric = (value, min, max, fallback) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(min, Math.min(max, n));
  };

  const avatarPathRaw = String(payload.avatarPath || payload.url || '').trim();
  const avatarPath = parseFileUrlPath(avatarPathRaw);
  if (!avatarPath) return failPreview('No intro avatar source path provided.');
  const isRemote = /^https?:\/\//i.test(avatarPath);
  if (!isRemote && !fs.existsSync(avatarPath)) return failPreview('Intro avatar source not found.');

  const width = Math.max(320, Math.round(Number(payload.width || 1080)));
  const height = Math.max(568, Math.round(Number(payload.height || 1920)));
  const startSec = Math.max(0, Number(payload.startSec || 0));
  const endSec = Math.max(startSec + 0.1, Number(payload.endSec || startSec + 2));
  const durationSec = Math.max(0.3, endSec - startSec);
  const modeRaw = String(payload.mode || 'greenscreen_black').toLowerCase();
  const mode = modeRaw === 'greenscreen' ? 'greenscreen' : 'greenscreen_black';
  const scale = clampNumeric(payload.scale, 0.18, 0.85, 0.52);
  const centerXPercent = clampNumeric(payload.centerXPercent, 0, 100, 50);
  const marginBottomPercent = clampNumeric(payload.marginBottomPercent, 0, 0.35, 0.03);
  const greenSimilarity = clampNumeric(payload.greenSimilarity, 0.05, 0.45, 0.24);
  const greenBlend = clampNumeric(payload.greenBlend, 0, 0.2, 0.06);
  const blackSimilarity = clampNumeric(payload.blackSimilarity, 0.005, 0.2, 0.04);
  const blackBlend = clampNumeric(payload.blackBlend, 0, 0.12, 0.01);
  const isImage = /\.(png|jpe?g|webp|bmp)$/i.test(String(avatarPath).split('?')[0] || '');
  const sourceType = String(payload.sourceType || '').trim().toLowerCase();
  const detectForeground = payload.detectForeground !== false && sourceType === 'scene_avatar_video' && !isImage;
  const previewStillOnly = payload.previewStillOnly !== false;
  let workingAvatarPath = avatarPath;
  let workingIsImage = isImage;
  let workingStartSec = startSec;
  let workingEndSec = endSec;
  let workingMaskPath = '';

  const downloadsDir = app.getPath('downloads');
  const outputNameRaw = String(payload.outputName || '').trim();
  const outputSafeBase =
    outputNameRaw.replace(/[^\w.-]+/g, '-').replace(/\.mp4$/i, '') || `tiktok-intro-mask-preview-${Date.now()}`;
  const outputPath = path.join(downloadsDir, `${outputSafeBase}.mp4`);
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'tiktok-intro-mask-'));
  let preparedColorPath = '';
  let preparedMaskPath = '';
  const progressRunId = String(payload.progressRunId || '').trim();
  const sendIntroMaskProgress = (percent = 0, message = '') => {
    if (!progressRunId) return;
    const wc = mainWindow?.webContents;
    if (!wc || wc.isDestroyed()) return;
    wc.send('tiktok-intro-mask-progress', {
      runId: progressRunId,
      percent: Math.max(0, Math.min(100, Number(percent) || 0)),
      message: String(message || '').trim()
    });
  };

  const runFfmpeg = (args) =>
    new Promise((resolve, reject) => {
      const ffmpegArgs = sanitizeFfmpegArgs(['-hide_banner', '-nostats', '-progress', 'pipe:2', ...args]);
      const proc = spawn('ffmpeg', ffmpegArgs, { stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      let maxSentPercent = 84;
      const startedAt = Date.now();
      const estMs = Math.max(2000, Math.min(18000, Math.round(durationSec * 1200)));
      const fallbackTimer = setInterval(() => {
        const elapsed = Date.now() - startedAt;
        const normalized = Math.max(0, Math.min(1, elapsed / estMs));
        const percent = Math.max(84, Math.min(96, Math.round(84 + normalized * 12)));
        if (percent <= maxSentPercent) return;
        maxSentPercent = percent;
        sendIntroMaskProgress(percent, 'Compositing mask frames');
      }, 350);
      proc.stderr.on('data', (data) => {
        const text = data.toString();
        err += text;
        const lines = text.split(/[\r\n]+/);
        lines.forEach((line) => {
          const clean = String(line || '').trim();
          if (!clean) return;
          if (!clean.startsWith('out_time_ms=') && !clean.startsWith('out_time_us=')) return;
          const raw = Number(clean.split('=')[1] || 0);
          if (!Number.isFinite(raw) || durationSec <= 0) return;
          const timeSec = clean.startsWith('out_time_us=') ? raw / 1000000 : raw / 1000000;
          const normalized = Math.max(0, Math.min(1, timeSec / durationSec));
          const percent = Math.max(84, Math.min(97, Math.round(84 + normalized * 13)));
          if (percent <= maxSentPercent) return;
          maxSentPercent = percent;
          sendIntroMaskProgress(percent, 'Compositing mask frames');
        });
      });
      proc.on('error', (error) => reject(error));
      proc.on('close', (code) => {
        clearInterval(fallbackTimer);
        if (code === 0) {
          sendIntroMaskProgress(99, 'Finalizing preview output');
          resolve();
        }
        else reject(new Error(err || `ffmpeg exited with ${code}`));
      });
    });
  const buildEncoderArgs = (encoder = '') => {
    if (encoder === 'libx264') {
      return ['-c:v', 'libx264', '-crf', '20'];
    }
    if (encoder === 'libopenh264') {
      return ['-c:v', 'libopenh264', '-b:v', '4M'];
    }
    if (encoder === 'mpeg4') {
      return ['-c:v', 'mpeg4', '-q:v', '4'];
    }
    return ['-c:v', 'libx264', '-crf', '20'];
  };
  const writeFilterScript = () => {
    if (detectForeground && workingMaskPath) {
      const scriptPath = path.join(tmpRoot, `filter-${Date.now()}-${Math.random().toString(36).slice(2)}.txt`);
      const chain =
        `[0:v]trim=start=${workingStartSec.toFixed(3)}:end=${workingEndSec.toFixed(3)},setpts=PTS-STARTPTS,format=rgba[color];` +
        `[1:v]trim=start=${workingStartSec.toFixed(3)}:end=${workingEndSec.toFixed(3)},setpts=PTS-STARTPTS,format=gray[mask];` +
        `[color][mask]alphamerge[avatar];` +
        `[avatar]null[vout]`;
      fs.writeFileSync(scriptPath, chain, 'utf8');
      return scriptPath;
    }
    const keyFilter =
      mode === 'greenscreen'
        ? `colorkey=0x00FF00:${Number(greenSimilarity || 0.24).toFixed(3)}:${Number(greenBlend || 0.06).toFixed(3)}`
        : `colorkey=0x00FF00:${Number(greenSimilarity || 0.24).toFixed(3)}:${Number(greenBlend || 0.06).toFixed(3)},` +
          `colorkey=0x000000:${Number(blackSimilarity || 0.04).toFixed(3)}:${Number(blackBlend || 0.01).toFixed(3)}`;
    const scriptPath = path.join(tmpRoot, `filter-${Date.now()}-${Math.random().toString(36).slice(2)}.txt`);
    const sourcePrep = workingIsImage
      ? `[0:v]format=rgba,${keyFilter}[avatar];`
      : `[0:v]trim=start=${workingStartSec.toFixed(3)}:end=${workingEndSec.toFixed(3)},setpts=PTS-STARTPTS,` +
        `format=rgba,${keyFilter}[avatar];`;
    const chain = sourcePrep + `[avatar]null[vout]`;
    fs.writeFileSync(scriptPath, chain, 'utf8');
    return scriptPath;
  };

  try {
    sendIntroMaskProgress(2, 'Preparing intro mask job');
    if (isRemote) {
      sendIntroMaskProgress(6, 'Caching remote Scene 1 source locally');
      const remoteExtMatch = String(avatarPath).match(/\.([a-z0-9]{2,6})(?:\?|$)/i);
      const remoteExt = remoteExtMatch ? `.${remoteExtMatch[1].toLowerCase()}` : '.mp4';
      const cachedPath = path.join(tmpRoot, `intro-remote-${Date.now()}${remoteExt}`);
      await downloadRemoteMediaToFile(avatarPath, cachedPath);
      workingAvatarPath = cachedPath;
      sendIntroMaskProgress(12, 'Remote source cached');
    }
    if (detectForeground) {
      sendIntroMaskProgress(14, 'Extracting avatar foreground');
      const extractedPath = path.join(tmpRoot, `intro-foreground-${Date.now()}.mp4`);
      const maskPath = path.join(tmpRoot, `intro-foreground-mask-${Date.now()}.mp4`);
      const extractionStartSec = previewStillOnly ? Math.min(endSec - 0.12, startSec + Math.min(0.35, durationSec * 0.28)) : startSec;
      const extractionEndSec = previewStillOnly
        ? Math.max(extractionStartSec + 0.12, Math.min(endSec, extractionStartSec + Math.min(0.85, durationSec)))
        : endSec;
      const extraction = await runAvatarForegroundExtraction({
        inputPath: workingAvatarPath,
        outputPath: extractedPath,
        maskOutputPath: maskPath,
        startSec: extractionStartSec,
        endSec: extractionEndSec,
        maxFrames: previewStillOnly ? 1 : 0,
        fastMode: previewStillOnly,
        onProgress: (pct, message = '') => {
          const hasPct = Number.isFinite(Number(pct));
          const mapped = hasPct ? Math.max(16, Math.min(74, Math.round(16 + (Number(pct || 0) / 100) * 58))) : 18;
          const suffix = String(message || '').trim();
          const label = suffix || (hasPct ? `Extracting avatar foreground (${Math.round(Number(pct || 0))}%)` : 'Extracting avatar foreground');
          sendIntroMaskProgress(mapped, label);
        }
      });
      workingAvatarPath = extraction.colorPath;
      workingMaskPath = extraction.maskPath || '';
      if (!workingMaskPath) {
        throw new Error('Foreground extraction did not produce a usable alpha mask.');
      }
      workingIsImage = false;
      workingStartSec = 0;
      workingEndSec = durationSec;
      sendIntroMaskProgress(76, 'Foreground extraction complete');
      const cacheDir = path.join(app.getPath('userData'), 'intro_mask_cache');
      fs.mkdirSync(cacheDir, { recursive: true });
      preparedColorPath = path.join(cacheDir, `intro-mask-color-${Date.now()}.mp4`);
      preparedMaskPath = path.join(cacheDir, `intro-mask-alpha-${Date.now()}.mp4`);
      fs.copyFileSync(workingAvatarPath, preparedColorPath);
      fs.copyFileSync(workingMaskPath, preparedMaskPath);
    }
    const encoders = ['libx264', 'libopenh264', 'mpeg4'];
    let lastErr = null;
      for (const encoder of encoders) {
      try {
        sendIntroMaskProgress(78, `Checking encoder (${encoder})`);
        const filterScript = writeFilterScript();
        sendIntroMaskProgress(82, 'Building mask filter graph');
        const args = [];
        if (workingIsImage) args.push('-loop', '1', '-t', durationSec.toFixed(3), '-i', workingAvatarPath);
        else args.push('-stream_loop', '-1', '-i', workingAvatarPath);
        if (detectForeground && workingMaskPath) {
          args.push('-stream_loop', '-1', '-i', workingMaskPath);
        }
        sendIntroMaskProgress(84, workingIsImage ? 'Rendering intro mask from image source' : 'Rendering intro mask from scene clip');
        args.push(
          '-filter_complex_script',
          filterScript,
          '-map',
          '[vout]',
          ...buildEncoderArgs(encoder),
          '-pix_fmt',
          'yuv420p',
          '-r',
          '30'
        );
        if (previewStillOnly) {
          args.push('-frames:v', '1');
        } else {
          args.push('-t', durationSec.toFixed(3));
        }
        args.push(
          '-movflags',
          '+faststart',
          '-y',
          outputPath
        );
        await runFfmpeg(args);
        sendIntroMaskProgress(100, 'Intro mask preview complete');
        return {
          success: true,
          path: outputPath,
          durationSec,
          preparedColorPath: preparedColorPath || '',
          preparedMaskPath: preparedMaskPath || '',
          pipelineVersion: CAPTION_RENDER_PIPELINE_VERSION
        };
      } catch (err) {
        lastErr = err;
      }
    }
    throw lastErr || new Error('No compatible encoder available for intro mask preview.');
  } catch (err) {
    sendIntroMaskProgress(0, 'Intro mask preview failed');
    return failPreview(err?.message || 'Intro mask preview failed.', {
      includePipelineVersion: true
    });
  } finally {
    try {
      fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch (_err) {}
  }
});

ipcMain.handle('publish-social-posts', async (_event, payload = {}) => {
  const videoPathRaw = String(payload.videoPath || '').trim();
  const caption = String(payload.caption || '').trim();
  const platforms = payload.platforms && typeof payload.platforms === 'object' ? payload.platforms : {};
  const creds = payload.credentials && typeof payload.credentials === 'object' ? payload.credentials : {};
  const kieApiKey = String(payload.kieApiKey || '').trim();

  if (!videoPathRaw) return { success: false, error: 'No video path provided.' };
  if (!caption) return { success: false, error: 'Caption is empty.' };

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const readJson = async (res) => {
    const text = await res.text();
    try {
      return text ? JSON.parse(text) : {};
    } catch (_err) {
      return { raw: text || '' };
    }
  };
  const ensurePublicVideoUrl = async () => {
    if (/^https?:\/\//i.test(videoPathRaw)) return videoPathRaw;
    if (!fs.existsSync(videoPathRaw)) throw new Error('Video file not found.');
    if (!kieApiKey) {
      throw new Error('Kie.ai key required to host local video for social posting.');
    }
    const uploaded = await uploadFileStream(videoPathRaw, {
      apiKey: kieApiKey,
      uploadPath: 'video/social'
    });
    return uploaded.url;
  };

  let publicVideoUrl = '';
  try {
    publicVideoUrl = await ensurePublicVideoUrl();
  } catch (err) {
    return {
      success: false,
      error: err?.message || 'Could not prepare video for social posting.',
      hostedVideoUrl: '',
      results: []
    };
  }
  const results = [];

  const publishTikTok = async () => {
    const token = String(creds.tiktokToken || '').trim();
    if (!token) throw new Error('TikTok token missing.');
    const body = {
      post_info: {
        title: caption.slice(0, 2200),
        privacy_level: 'PUBLIC_TO_EVERYONE',
        disable_comment: false,
        disable_duet: false,
        disable_stitch: false
      },
      source_info: {
        source: 'PULL_FROM_URL',
        video_url: publicVideoUrl
      }
    };
    const res = await fetch('https://open.tiktokapis.com/v2/post/publish/video/init/', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });
    const parsed = await readJson(res);
    if (!res.ok) {
      throw new Error(parsed?.error?.message || parsed?.message || `TikTok publish failed (${res.status})`);
    }
    return {
      postId: parsed?.data?.publish_id || parsed?.data?.task_id || '',
      raw: parsed
    };
  };

  const publishInstagram = async () => {
    const token = String(creds.instagramToken || '').trim();
    const userId = String(creds.instagramUserId || '').trim();
    if (!token || !userId) throw new Error('Instagram token/user ID missing.');
    const createParams = new URLSearchParams({
      media_type: 'REELS',
      video_url: publicVideoUrl,
      caption,
      access_token: token
    });
    const createRes = await fetch(`https://graph.facebook.com/v21.0/${encodeURIComponent(userId)}/media`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: createParams
    });
    const createJson = await readJson(createRes);
    if (!createRes.ok || !createJson?.id) {
      throw new Error(createJson?.error?.message || `Instagram media create failed (${createRes.status})`);
    }
    const creationId = String(createJson.id);

    let statusCode = 'IN_PROGRESS';
    for (let i = 0; i < 40; i += 1) {
      await sleep(3000);
      const statusRes = await fetch(
        `https://graph.facebook.com/v21.0/${encodeURIComponent(creationId)}?fields=status_code,status&access_token=${encodeURIComponent(token)}`
      );
      const statusJson = await readJson(statusRes);
      statusCode = String(statusJson?.status_code || statusJson?.status || '').toUpperCase();
      if (statusCode === 'FINISHED' || statusCode === 'PUBLISHED') break;
      if (statusCode === 'ERROR' || statusCode === 'FAILED') {
        throw new Error(statusJson?.error?.message || 'Instagram media processing failed.');
      }
    }

    const publishParams = new URLSearchParams({
      creation_id: creationId,
      access_token: token
    });
    const publishRes = await fetch(`https://graph.facebook.com/v21.0/${encodeURIComponent(userId)}/media_publish`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: publishParams
    });
    const publishJson = await readJson(publishRes);
    if (!publishRes.ok || !publishJson?.id) {
      throw new Error(publishJson?.error?.message || `Instagram publish failed (${publishRes.status})`);
    }
    return { postId: String(publishJson.id), raw: publishJson };
  };

  const publishFacebook = async () => {
    const token = String(creds.facebookToken || '').trim();
    const pageId = String(creds.facebookPageId || '').trim();
    if (!token || !pageId) throw new Error('Facebook token/page ID missing.');
    const params = new URLSearchParams({
      file_url: publicVideoUrl,
      description: caption,
      access_token: token
    });
    const res = await fetch(`https://graph.facebook.com/v21.0/${encodeURIComponent(pageId)}/videos`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params
    });
    const parsed = await readJson(res);
    if (!res.ok || !parsed?.id) {
      throw new Error(parsed?.error?.message || `Facebook video publish failed (${res.status})`);
    }
    return { postId: String(parsed.id), raw: parsed };
  };

  const runPublish = async (platform, enabled, fn) => {
    if (!enabled) return;
    try {
      const info = await fn();
      results.push({
        platform,
        success: true,
        postId: info?.postId || '',
        detail: info?.raw || {}
      });
    } catch (err) {
      results.push({
        platform,
        success: false,
        error: err.message || 'Publish failed.'
      });
    }
  };

  try {
    await runPublish('tiktok', !!platforms.tiktok, publishTikTok);
    await runPublish('instagram', !!platforms.instagram, publishInstagram);
    await runPublish('facebook', !!platforms.facebook, publishFacebook);
    return {
      success: true,
      hostedVideoUrl: publicVideoUrl,
      results
    };
  } catch (err) {
    return {
      success: false,
      error: err.message || 'Publish workflow failed.',
      hostedVideoUrl: publicVideoUrl,
      results
    };
  }
});

ipcMain.handle('extract-video-frames', async (_event, payload = {}) => {
  const { url = '', timestamps = [] } = payload;
  if (!url) return { success: false, error: 'No video URL provided.' };
  const times = Array.isArray(timestamps) ? timestamps.filter((t) => Number.isFinite(t) && t >= 0) : [];
  if (!times.length) return { success: false, error: 'No timestamps provided.' };
  const tmpDir = path.join(os.tmpdir(), 'amelia_frames');
  fs.mkdirSync(tmpDir, { recursive: true });
  const results = [];
  try {
    for (let i = 0; i < times.length; i += 1) {
      const stamp = Date.now();
      const outPath = path.join(tmpDir, `frame-${stamp}-${i}.jpg`);
      await new Promise((resolve, reject) => {
        const args = [
          '-y',
          '-loglevel',
          'error',
          '-ss',
          String(times[i]),
          '-i',
          url,
          '-frames:v',
          '1',
          '-vf',
          'scale=512:-1',
          '-q:v',
          '2',
          outPath
        ];
        const proc = spawn('ffmpeg', sanitizeFfmpegArgs(args), { stdio: ['ignore', 'pipe', 'pipe'] });
        let err = '';
        proc.stderr.on('data', (data) => {
          err += data.toString();
        });
        proc.on('error', (error) => reject(error));
        proc.on('close', (code) => {
          if (code === 0) resolve();
          else reject(new Error(err || `ffmpeg exited with ${code}`));
        });
      });
      const buffer = fs.readFileSync(outPath);
      const dataUrl = `data:image/jpeg;base64,${buffer.toString('base64')}`;
      results.push(dataUrl);
      fs.unlinkSync(outPath);
    }
    return { success: true, frames: results };
  } catch (err) {
    return { success: false, error: err.message };
  }
});

ipcMain.handle('download-youtube-clip', async (_event, payload = {}) => {
  const { url = '', sceneId = '', runId = '' } = payload;
  const getExportCtx = () => {
    const key = String(runId || '').trim();
    if (!key) return null;
    return activeTikTokExports.get(key) || null;
  };
  const isCancelled = () => {
    const ctx = getExportCtx();
    return !!ctx?.cancelled;
  };
  const ensureNotCancelled = () => {
    if (isCancelled()) throw new Error('Export reset by user.');
  };
  if (!url) return { success: false, error: 'No YouTube URL provided.' };
  try {
    ensureNotCancelled();
    if (!ytdl.validateURL(url)) {
      return { success: false, error: 'Invalid YouTube URL.' };
    }
    fs.mkdirSync(YOUTUBE_CACHE_DIR, { recursive: true });
    const stamp = Date.now();
    const safeScene = String(sceneId || 'clip').replace(/[^\w.-]+/g, '_');
    const videoId = ytdl.getURLVideoID(url);
    const ipv4Agent = new Agent({ connect: { family: 4 } });
    const sendProgress = (percent, line) => {
      if (!mainWindow) return;
      mainWindow.webContents.send('youtube-download-progress', {
        sceneId,
        percent,
        line
      });
    };
    const MAX_ATTEMPTS = 3;
    const ATTEMPT_TIMEOUT_MS = 4 * 60 * 1000;
    const STALL_TIMEOUT_MS = 20000;
    const withTimeout = (promise, ms, label) =>
      Promise.race([
        promise,
        new Promise((_, reject) => {
          const t = setTimeout(() => {
            clearTimeout(t);
            reject(new Error(label || 'Timed out'));
          }, ms);
        })
      ]);
    const downloadFromUrl = async (downloadUrl, extHint = 'mp4') => {
      const outPath = path.join(YOUTUBE_CACHE_DIR, `${safeScene}-${stamp}.${extHint}`);
      const controller = new AbortController();
      const ctx = getExportCtx();
      if (ctx) ctx.fetchControllers.add(controller);
      let lastChunkAt = Date.now();
      const stallTimer = setInterval(() => {
        if (isCancelled()) {
          sendProgress(0, 'Export reset. Cancelling download…');
          controller.abort();
          return;
        }
        if (Date.now() - lastChunkAt > STALL_TIMEOUT_MS) {
          sendProgress(0, 'Download stalled. Aborting for retry…');
          controller.abort();
        }
      }, 5000);
      try {
        sendProgress(0, 'Connecting to video stream…');
        const res = await withTimeout(
          fetch(downloadUrl, { redirect: 'follow', signal: controller.signal, dispatcher: ipv4Agent }),
          30000,
          'Video download timed out.'
        );
        if (!res.ok || !res.body) {
          clearInterval(stallTimer);
          throw new Error(`Download failed (HTTP ${res.status}).`);
        }
        const total = Number(res.headers.get('content-length')) || 0;
        const file = fs.createWriteStream(outPath);
        let downloaded = 0;
        let lastReport = 0;
        for await (const chunk of res.body) {
          ensureNotCancelled();
          lastChunkAt = Date.now();
          downloaded += chunk.length;
          if (!file.write(chunk)) {
            await new Promise((resolve) => file.once('drain', resolve));
          }
          if (total) {
            const percent = (downloaded / total) * 100;
            sendProgress(percent, `Downloading… ${percent.toFixed(1)}%`);
          } else if (Date.now() - lastReport > 1000) {
            lastReport = Date.now();
            sendProgress(0, `Downloading… ${Math.round(downloaded / 1024 / 1024)}MB`);
          }
        }
        clearInterval(stallTimer);
        if (ctx) ctx.fetchControllers.delete(controller);
        await new Promise((resolve, reject) => {
          file.end(() => resolve());
          file.on('error', reject);
        });
        if (!fs.existsSync(outPath)) {
          throw new Error('Download did not produce a file.');
        }
        return outPath;
      } catch (err) {
        clearInterval(stallTimer);
        if (ctx) ctx.fetchControllers.delete(controller);
        if (fs.existsSync(outPath)) {
          fs.unlinkSync(outPath);
        }
        throw err;
      }
    };

    const tryYtdlCore = async (attemptLabel = '') => {
      sendProgress(0, `${attemptLabel}Fetching YouTube metadata…`);
      const requestOptions = {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
          'Accept-Language': 'en-US,en;q=0.9'
        },
        family: 4
      };
      const info = await withTimeout(
        ytdl.getInfo(url, {
          requestOptions
        }),
        30000,
        'YouTube metadata timed out.'
      );
      sendProgress(0, `${attemptLabel}Selecting YouTube format…`);
      let format = ytdl.chooseFormat(info.formats, { quality: 'highest', filter: 'audioandvideo' });
      if (!format || !format.url) {
        format = ytdl.chooseFormat(info.formats, { quality: 'highestvideo', filter: 'videoonly' });
      }
      if (!format || !format.url) {
        throw new Error('No downloadable YouTube format found.');
      }
      const container = format.container || (format.mimeType || '').split('/')[1]?.split(';')[0] || 'mp4';
      const outPath = path.join(YOUTUBE_CACHE_DIR, `${safeScene}-${stamp}.${container}`);
      sendProgress(0, `${attemptLabel}YouTube download started (${container}).`);
      try {
        await withTimeout(
          new Promise((resolve, reject) => {
            const stream = ytdl(url, {
              format,
              highWaterMark: 1 << 25,
              requestOptions
            });
            const file = fs.createWriteStream(outPath);
            let lastChunkAt = Date.now();
            const stallTimer = setInterval(() => {
              if (isCancelled()) {
                sendProgress(0, 'Export reset. Cancelling download…');
                stream.destroy(new Error('Export reset by user.'));
                return;
              }
              if (Date.now() - lastChunkAt > STALL_TIMEOUT_MS) {
                sendProgress(0, 'Download stalled. Aborting for retry…');
                stream.destroy(new Error('Download stalled.'));
              }
            }, 5000);
            stream.on('response', () => {
              sendProgress(0, 'Downloading stream connected…');
            });
            stream.on('info', (_info, currentFormat) => {
              const label = currentFormat?.qualityLabel || currentFormat?.itag || 'stream';
              sendProgress(0, `Downloading ${label}…`);
            });
            stream.on('progress', (_chunkLength, downloaded, total) => {
              lastChunkAt = Date.now();
              const percent = total ? (downloaded / total) * 100 : 0;
              sendProgress(percent, `Downloading… ${percent.toFixed(1)}%`);
            });
            stream.on('data', () => {
              lastChunkAt = Date.now();
            });
            stream.on('error', (error) => {
              clearInterval(stallTimer);
              reject(error);
            });
            file.on('finish', () => {
              clearInterval(stallTimer);
              resolve();
            });
            file.on('error', (error) => {
              clearInterval(stallTimer);
              reject(error);
            });
            stream.pipe(file);
          }),
          ATTEMPT_TIMEOUT_MS,
          'YouTube download timed out.'
        );
        if (!fs.existsSync(outPath)) {
          throw new Error('YouTube download did not produce a file.');
        }
        return outPath;
      } catch (err) {
        if (fs.existsSync(outPath)) {
          fs.unlinkSync(outPath);
        }
        throw err;
      }
    };

    const parseJsonOrThrow = async (res, sourceLabel = 'Fallback') => {
      const contentType = String(res.headers.get('content-type') || '').toLowerCase();
      const text = await res.text();
      if (!contentType.includes('application/json')) {
        const preview = text.slice(0, 80).replace(/\s+/g, ' ').trim();
        throw new Error(`${sourceLabel} returned non-JSON (${res.status})${preview ? `: ${preview}` : ''}`);
      }
      try {
        return JSON.parse(text);
      } catch (err) {
        const preview = text.slice(0, 80).replace(/\s+/g, ' ').trim();
        throw new Error(
          `${sourceLabel} JSON parse failed${preview ? `: ${preview}` : ''}${
            err?.message ? ` (${err.message})` : ''
          }`
        );
      }
    };

    const fallbackFetchOptions = {
      dispatcher: ipv4Agent,
      headers: {
        'User-Agent':
          'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
        Accept: 'application/json,text/plain,*/*',
        'Accept-Language': 'en-US,en;q=0.9'
      }
    };

    const tryInvidious = async () => {
      const instances = [
        'https://yewtu.be',
        'https://vid.puffyan.us',
        'https://invidious.fdn.fr',
        'https://invidious.privacyredirect.com'
      ];
      let lastErr = null;
      for (const base of instances) {
        try {
          sendProgress(0, `Fallback: checking ${base}…`);
          const apiUrl = `${base}/api/v1/videos/${videoId}`;
          const res = await withTimeout(
            fetch(apiUrl, fallbackFetchOptions),
            15000,
            'Invidious metadata timed out.'
          );
          if (!res.ok) throw new Error(`Invidious HTTP ${res.status}`);
          const data = await parseJsonOrThrow(res, `Invidious ${base}`);
          const formats = []
            .concat(data.formatStreams || [])
            .concat(data.adaptiveFormats || [])
            .filter((f) => f && f.url && String(f.type || '').includes('video/'));
          if (!formats.length) throw new Error('No formats from Invidious.');
          const mp4 = formats.filter((f) => String(f.type || '').includes('mp4'));
          const pickFrom = mp4.length ? mp4 : formats;
          pickFrom.sort(
            (a, b) =>
              Number(b.height || 0) - Number(a.height || 0) ||
              Number(b.bitrate || 0) - Number(a.bitrate || 0)
          );
          const picked = pickFrom[0];
          const extHint = String(picked.type || '').includes('webm') ? 'webm' : 'mp4';
          sendProgress(0, `Fallback download from ${base}…`);
          return await downloadFromUrl(picked.url, extHint);
        } catch (err) {
          lastErr = err;
          sendProgress(0, `Fallback failed at ${base}: ${err.message}`);
        }
      }
      throw lastErr || new Error('Invidious fallback failed.');
    };

    const tryPiped = async () => {
      const instances = [
        'https://piped.video',
        'https://pipedapi.kavin.rocks',
        'https://api.piped.video'
      ];
      let lastErr = null;
      for (const base of instances) {
        try {
          sendProgress(0, `Fallback: checking ${base}…`);
          const apiUrl = `${base}/api/v1/streams/${videoId}`;
          const res = await withTimeout(
            fetch(apiUrl, fallbackFetchOptions),
            15000,
            'Piped metadata timed out.'
          );
          if (!res.ok) throw new Error(`Piped HTTP ${res.status}`);
          const data = await parseJsonOrThrow(res, `Piped ${base}`);
          const formats = (data.videoStreams || []).filter((f) => f && f.url);
          if (!formats.length) throw new Error('No formats from Piped.');
          const mp4 = formats.filter((f) => String(f.format || f.mimeType || '').includes('mp4'));
          const pickFrom = mp4.length ? mp4 : formats;
          pickFrom.sort(
            (a, b) =>
              Number(b.height || 0) - Number(a.height || 0) ||
              Number(b.bitrate || 0) - Number(a.bitrate || 0)
          );
          const picked = pickFrom[0];
          const extHint = String(picked.format || picked.mimeType || '').includes('webm') ? 'webm' : 'mp4';
          sendProgress(0, `Fallback download from ${base}…`);
          return await downloadFromUrl(picked.url, extHint);
        } catch (err) {
          lastErr = err;
          sendProgress(0, `Fallback failed at ${base}: ${err.message}`);
        }
      }
      throw lastErr || new Error('Piped fallback failed.');
    };

    const tryYtDlp = async (attemptLabel = '') => {
      const outputTemplate = path.join(YOUTUBE_CACHE_DIR, `${safeScene}-${stamp}.%(ext)s`);
      sendProgress(0, `${attemptLabel}trying yt-dlp fallback…`);
      await withTimeout(
        new Promise((resolve, reject) => {
          const args = [
            '--no-playlist',
            '--restrict-filenames',
            '--merge-output-format',
            'mp4',
            '-f',
            'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b',
            '-o',
            outputTemplate,
            url
          ];
          const proc = spawn('yt-dlp', args, { stdio: ['ignore', 'pipe', 'pipe'] });
          let stderr = '';
          proc.stdout.on('data', (data) => {
            const line = String(data || '').trim();
            if (line) sendProgress(0, `${attemptLabel}yt-dlp: ${line.slice(0, 140)}`);
          });
          proc.stderr.on('data', (data) => {
            const text = String(data || '');
            stderr += text;
            const line = text.split('\n').map((part) => part.trim()).filter(Boolean).pop();
            if (line) sendProgress(0, `${attemptLabel}yt-dlp: ${line.slice(0, 140)}`);
          });
          proc.on('error', (err) => reject(err));
          proc.on('close', (code) => {
            if (code === 0) return resolve();
            reject(new Error(stderr.trim() || `yt-dlp exited with ${code}`));
          });
        }),
        ATTEMPT_TIMEOUT_MS,
        'yt-dlp timed out.'
      );
      const prefix = `${safeScene}-${stamp}.`;
      const candidates = fs
        .readdirSync(YOUTUBE_CACHE_DIR)
        .filter((name) => name.startsWith(prefix))
        .map((name) => path.join(YOUTUBE_CACHE_DIR, name))
        .filter((full) => fs.existsSync(full))
        .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
      if (!candidates.length) {
        throw new Error('yt-dlp completed but no output file was found.');
      }
      return candidates[0];
    };

    let outPath = '';
    let lastError = null;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
      ensureNotCancelled();
      const label = `Attempt ${attempt}/${MAX_ATTEMPTS}: `;
      try {
        sendProgress(0, `${label}direct YouTube download…`);
        outPath = await tryYtdlCore(label);
        lastError = null;
        break;
      } catch (err) {
        lastError = err;
        sendProgress(0, `${label}direct failed: ${err.message}`);
        try {
          sendProgress(0, `${label}trying Invidious fallback…`);
          outPath = await tryInvidious();
          lastError = null;
          break;
        } catch (err2) {
          lastError = err2;
          sendProgress(0, `${label}Invidious failed: ${err2.message}`);
          try {
            sendProgress(0, `${label}trying Piped fallback…`);
            outPath = await tryPiped();
            lastError = null;
            break;
          } catch (err3) {
            lastError = err3;
            sendProgress(0, `${label}Piped failed: ${err3.message}`);
            try {
              outPath = await tryYtDlp(label);
              lastError = null;
              break;
            } catch (err4) {
              lastError = err4;
              sendProgress(0, `${label}yt-dlp failed: ${err4.message}`);
            }
          }
        }
      }
      if (attempt < MAX_ATTEMPTS) {
        const waitMs = 1200 * attempt;
        sendProgress(0, `${label}retrying in ${Math.round(waitMs / 1000)}s…`);
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
    }
    if (!outPath) {
      throw lastError || new Error('YouTube download failed after retries.');
    }

    return { success: true, path: outPath };
  } catch (err) {
    return { success: false, error: err.message || 'YouTube download failed.' };
  }
});

ipcMain.handle('select-image-save-target', async (_event, payload = {}) => {
  const preferred = String(payload.defaultPath || '').trim();
  const defaultPath = preferred || app.getPath('downloads');
  try {
    const { canceled, filePaths } = await dialog.showOpenDialog({
      title: 'Choose save folder or last saved image',
      defaultPath,
      properties: ['openFile', 'openDirectory', 'createDirectory'],
      filters: [
        { name: 'Images', extensions: ['webp', 'png', 'jpg', 'jpeg'] },
        { name: 'All Files', extensions: ['*'] }
      ]
    });
    if (canceled || !Array.isArray(filePaths) || !filePaths.length) {
      return { success: false, canceled: true };
    }
    const targetPath = filePaths[0];
    const stats = fs.statSync(targetPath);
    const targetType = stats.isFile() ? 'file' : 'directory';
    const directoryPath = stats.isFile() ? path.dirname(targetPath) : targetPath;
    return { success: true, targetPath, targetType, directoryPath };
  } catch (err) {
    return { success: false, error: err.message || 'Failed to choose save target.' };
  }
});

ipcMain.handle('get-image-sequence-next-index', async (_event, payload = {}) => {
  const targetPath = String(payload.targetPath || '').trim();
  if (!targetPath) return { success: false, error: 'No save target path provided.' };

  const requestedType = String(payload.targetType || '').trim().toLowerCase();
  let targetType = requestedType === 'file' || requestedType === 'directory' ? requestedType : '';
  let directoryPath = targetPath;
  try {
    const stats = fs.statSync(targetPath);
    if (stats.isFile()) {
      targetType = 'file';
      directoryPath = path.dirname(targetPath);
    } else if (stats.isDirectory()) {
      targetType = 'directory';
      directoryPath = targetPath;
    } else {
      return { success: false, error: 'Save target must be a file or directory.' };
    }
  } catch (err) {
    return { success: false, error: err.message || 'Could not access save target.' };
  }

  const prefix = sanitizeImageSequencePrefix(payload.prefix || 'image');
  const extRaw = String(payload.extension || 'webp')
    .trim()
    .replace(/^\./, '')
    .toLowerCase();
  const extension = /^[a-z0-9]{2,6}$/.test(extRaw) ? extRaw : 'webp';
  const filenamePattern = new RegExp(`^${escapeRegExp(prefix)}_(\\d+)\\.${escapeRegExp(extension)}$`, 'i');

  let maxIndex = -1;
  try {
    const names = fs.readdirSync(directoryPath);
    for (const name of names) {
      const match = String(name || '').match(filenamePattern);
      if (!match) continue;
      const index = Number.parseInt(match[1], 10);
      if (Number.isFinite(index) && index > maxIndex) {
        maxIndex = index;
      }
    }
  } catch (err) {
    return { success: false, error: err.message || 'Could not inspect save folder.' };
  }

  return {
    success: true,
    targetType,
    directoryPath,
    prefix,
    extension,
    maxIndex,
    nextIndex: maxIndex + 1
  };
});

ipcMain.handle('save-image-file', async (_event, payload = {}) => {
  const directoryPath = String(payload.directoryPath || '').trim();
  if (!directoryPath) return { success: false, error: 'No directory path provided.' };

  const filename = sanitizeImageFilename(payload.filename || 'image.webp');
  const rawData = payload.data;
  let buffer = null;

  if (ArrayBuffer.isView(rawData)) {
    buffer = Buffer.from(rawData.buffer, rawData.byteOffset, rawData.byteLength);
  } else if (rawData instanceof ArrayBuffer) {
    buffer = Buffer.from(rawData);
  } else if (Array.isArray(rawData)) {
    buffer = Buffer.from(rawData);
  } else {
    return { success: false, error: 'Invalid image data.' };
  }

  try {
    fs.mkdirSync(directoryPath, { recursive: true });
    const filePath = path.join(directoryPath, filename);
    fs.writeFileSync(filePath, buffer);
    return { success: true, path: filePath };
  } catch (err) {
    return { success: false, error: err.message || 'Failed to save image file.' };
  }
});

ipcMain.handle('save-remote-file', async (_event, payload = {}) => {
  const { url = '', suggestedName = 'image.png' } = payload;
  if (!url) return { success: false, error: 'No URL provided' };
  try {
    const parsed = new URL(url);
    const nameFromUrl = path.basename(parsed.pathname) || suggestedName;
    const defaultPath = path.join(app.getPath('downloads'), nameFromUrl);
    const { canceled, filePath } = await dialog.showSaveDialog({
      defaultPath,
      filters: [
        { name: 'Images', extensions: ['png', 'jpg', 'jpeg', 'webp'] },
        { name: 'All Files', extensions: ['*'] }
      ]
    });
    if (canceled || !filePath) return { success: false, canceled: true };

    const res = await fetch(url);
    if (!res.ok) {
      return { success: false, error: `HTTP ${res.status}` };
    }
    const buf = Buffer.from(await res.arrayBuffer());
    fs.writeFileSync(filePath, buf);
    return { success: true, path: filePath };
  } catch (err) {
    return { success: false, error: err.message };
  }
});

ipcMain.handle('cancel-tiktok-export', async (_event, payload = {}) => {
  const runId = String(payload?.runId || '').trim();
  if (!runId) return { success: false, error: 'Missing runId.' };
  const ctx = activeTikTokExports.get(runId);
  if (!ctx) return { success: true, cancelled: 0 };
  ctx.cancelled = true;
  let cancelled = 0;
  for (const proc of ctx.ffmpegProcs || []) {
    try {
      proc.kill('SIGKILL');
      cancelled += 1;
    } catch (_err) {}
  }
  for (const controller of ctx.fetchControllers || []) {
    try {
      controller.abort();
    } catch (_err) {}
  }
  return { success: true, cancelled };
});

ipcMain.handle('export-tiktok-timeline', async (_event, payload = {}) => {
  const scenes = Array.isArray(payload.scenes) ? payload.scenes : [];
  if (!scenes.length) return { success: false, error: 'No scenes to export.' };
  const missing = scenes.filter((scene) => !scene.clip?.url);
  if (missing.length) {
    return { success: false, error: `Missing clips for ${missing.length} scene(s).` };
  }
  const width = Number(payload.width || 1080);
  const height = Number(payload.height || 1920);
  const fps = Number(payload.fps || 30);
  const audioPath = payload.audioPath || '';
  const audioMode = String(payload.audioMode || '').trim().toLowerCase();

  const downloadsDir = app.getPath('downloads');
  const safeName = String(payload.outputName || '').trim().replace(/[^\w.-]+/g, '-');
  const baseName = safeName || `tiktok-export-${Date.now()}.mp4`;
  const outputPath = path.join(downloadsDir, baseName.endsWith('.mp4') ? baseName : `${baseName}.mp4`);

  const progressTag = String(payload.progressTag || '').trim() || 'render';
  const progressRunId = String(payload.progressRunId || '').trim() || '';
  const exportDebugInfo = {
    audioPath: String(audioPath || '').trim(),
    audioMode: audioMode || 'default',
    sceneCount: scenes.length,
    keying: {
      greenSimilarity: Number(payload.overlay_green_similarity ?? 0.24),
      greenBlend: Number(payload.overlay_green_blend ?? 0.06),
      blackSimilarity: Number(payload.overlay_black_similarity ?? 0.04),
      blackBlend: Number(payload.overlay_black_blend ?? 0.01)
    }
  };
  const runIdForCancel = progressRunId;
  const exportCtx = {
    cancelled: false,
    ffmpegProcs: new Set(),
    fetchControllers: new Set()
  };
  if (runIdForCancel) {
    activeTikTokExports.set(runIdForCancel, exportCtx);
  }
  const isExportCancelled = () => !!exportCtx.cancelled;
  const ensureExportNotCancelled = () => {
    if (isExportCancelled()) throw new Error('Export reset by user.');
  };
  try {
  const totalDurationSec = scenes.reduce((sum, scene) => {
    const direct = Number(scene.duration_sec || 0);
    if (Number.isFinite(direct) && direct > 0) return sum + direct;
    const diff = (Number(scene.end_ms || 0) - Number(scene.start_ms || 0)) / 1000;
    return sum + (Number.isFinite(diff) && diff > 0 ? diff : 0);
  }, 0);
  const sendExportProgress = (percent = 0, message = '', phase = progressTag) => {
    if (!mainWindow?.webContents) return;
    mainWindow.webContents.send('tiktok-export-progress', {
      runId: progressRunId,
      tag: progressTag,
      phase,
      percent: Math.max(0, Math.min(100, Number(percent) || 0)),
      message: String(message || '')
    });
  };
  const parseFfmpegTimeSec = (line = '') => {
    const match = /time=(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(line);
    if (!match) return null;
    const h = Number(match[1] || 0);
    const m = Number(match[2] || 0);
    const s = Number(match[3] || 0);
    return h * 3600 + m * 60 + s;
  };
  const parseFfmpegProgressSec = (text = '') => {
    let best = null;
    const regexes = [
      /out_time=(\d+):(\d+):(\d+(?:\.\d+)?)/g,
      /time=(\d+):(\d+):(\d+(?:\.\d+)?)/g
    ];
    regexes.forEach((rx) => {
      let match = null;
      while ((match = rx.exec(text)) !== null) {
        const h = Number(match[1] || 0);
        const m = Number(match[2] || 0);
        const s = Number(match[3] || 0);
        const total = h * 3600 + m * 60 + s;
        if (Number.isFinite(total) && (best === null || total > best)) best = total;
      }
    });
    let msMatch = null;
    const outTimeMsRx = /out_time_ms=(\d+)/g;
    while ((msMatch = outTimeMsRx.exec(text)) !== null) {
      const raw = Number(msMatch[1] || 0);
      if (!Number.isFinite(raw) || raw <= 0) continue;
      // ffmpeg progress key is named *_ms but value is microseconds in most builds.
      const sec = raw >= 1000000 ? raw / 1000000 : raw / 1000;
      if (best === null || sec > best) best = sec;
    }
    return best;
  };
  const probeHasAudioStream = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve(false);
        return;
      }
      let out = '';
      const proc = spawn(
        'ffprobe',
        ['-v', 'error', '-select_streams', 'a:0', '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', targetPath],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve(false));
      proc.on('close', (code) => {
        resolve(code === 0 && /audio/i.test(out));
      });
    });
  const probeAudioDurationSec = (targetPath = '') =>
    new Promise((resolve) => {
      if (!targetPath || !fs.existsSync(targetPath)) {
        resolve(0);
        return;
      }
      let out = '';
      const proc = spawn(
        'ffprobe',
        [
          '-v',
          'error',
          '-select_streams',
          'a:0',
          '-show_entries',
          'stream=duration',
          '-of',
          'default=noprint_wrappers=1:nokey=1',
          targetPath
        ],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      );
      proc.stdout.on('data', (chunk) => {
        out += String(chunk || '');
      });
      proc.on('error', () => resolve(0));
      proc.on('close', () => {
        const value = Number(String(out || '').trim().split('\n').pop() || 0);
        resolve(Number.isFinite(value) && value > 0 ? value : 0);
      });
    });
  const ensureVoiceoverAudioInOutput = async (sourceAudioPath = audioPath) => {
    const sourcePath = String(sourceAudioPath || '').trim();
    if (!sourcePath || !fs.existsSync(sourcePath) || !fs.existsSync(outputPath)) return;
    const hasAudio = await probeHasAudioStream(outputPath);
    const srcAudioDuration = await probeAudioDurationSec(sourcePath);
    const outAudioDuration = hasAudio ? await probeAudioDurationSec(outputPath) : 0;
    const needsRepair = !hasAudio || (srcAudioDuration > 0.25 && outAudioDuration + 0.25 < srcAudioDuration);
    if (!needsRepair) return;
    const reason = !hasAudio
      ? 'Output missing audio.'
      : `Output audio is too short (${outAudioDuration.toFixed(2)}s < ${srcAudioDuration.toFixed(2)}s).`;
    sendExportProgress(99, `${reason} Remuxing ElevenLabs voiceover...`, progressTag);
    const repairedPath = path.join(
      app.getPath('temp'),
      `tiktok-audiofix-${Date.now()}-${Math.random().toString(16).slice(2, 8)}.mp4`
    );
    try {
      await runFfmpeg([
        '-i',
        outputPath,
        '-i',
        sourcePath,
        '-map',
        '0:v:0',
        '-map',
        '1:a:0',
        '-c:v',
        'copy',
        '-c:a',
        'aac',
        '-b:a',
        '192k',
        '-af',
        'aresample=async=1:first_pts=0',
        '-movflags',
        '+faststart',
        '-y',
        repairedPath
      ]);
      fs.copyFileSync(repairedPath, outputPath);
    } finally {
      try {
        if (fs.existsSync(repairedPath)) fs.unlinkSync(repairedPath);
      } catch (_err) {}
    }
  };

  const probeVideoEncoder = async (enc = '') => {
    const probeArgs = [
      '-f',
      'lavfi',
      '-i',
      `color=c=black:s=${width}x${height}:d=0.2`,
      '-frames:v',
      '1',
      '-c:v',
      enc
    ];
    if (enc === 'libx264') {
      probeArgs.splice(8, 0, '-preset', 'ultrafast');
    } else if (enc === 'mpeg4') {
      probeArgs.splice(8, 0, '-q:v', '8');
    }
    probeArgs.push('-f', 'null', '-');
    await runFfmpeg(probeArgs, {
      phase: progressTag,
      messagePrefix: `Checking encoder (${enc})...`,
      stallTimeoutMs: 12000,
      hardTimeoutMs: 45000
    });
  };

  const selectPrimaryEncoder = async () => {
    const candidates = ['libx264', 'libopenh264', 'mpeg4'];
    let lastErr = null;
    for (const enc of candidates) {
      try {
        sendExportProgress(1, `Checking encoder (${enc})...`, progressTag);
        await probeVideoEncoder(enc);
        return enc;
      } catch (err) {
        lastErr = err;
      }
    }
    throw lastErr || new Error('No working video encoder found.');
  };

  const runFfmpeg = (args, options = {}) =>
    new Promise((resolve, reject) => {
      ensureExportNotCancelled();
      const durationSec = Number(options.durationSec || 0);
      const phase = options.phase || progressTag;
      const messagePrefix = options.messagePrefix || '';
      const stallTimeoutMs = Math.max(15000, Number(options.stallTimeoutMs || 90000));
      const hardTimeoutMs = Math.max(stallTimeoutMs + 10000, Number(options.hardTimeoutMs || 20 * 60 * 1000));
      let lastPct = -1;
      const ffmpegArgs = sanitizeFfmpegArgs(['-hide_banner', '-nostats', '-progress', 'pipe:2', ...args]);
      const proc = spawn('ffmpeg', ffmpegArgs, { stdio: ['ignore', 'pipe', 'pipe'] });
      exportCtx.ffmpegProcs.add(proc);
      let err = '';
      let settled = false;
      let hardTimer = null;
      let stallTimer = null;
      let heartbeatTimer = null;
      const clearTimers = () => {
        if (hardTimer) clearTimeout(hardTimer);
        if (stallTimer) clearTimeout(stallTimer);
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        hardTimer = null;
        stallTimer = null;
        heartbeatTimer = null;
        exportCtx.ffmpegProcs.delete(proc);
      };
      const killForTimeout = (reason) => {
        if (settled) return;
        settled = true;
        clearTimers();
        try {
          proc.kill('SIGKILL');
        } catch (_err) {}
        reject(new Error(reason));
      };
      const bumpStallTimer = () => {
        if (settled) return;
        if (stallTimer) clearTimeout(stallTimer);
        stallTimer = setTimeout(() => {
          killForTimeout(
            `ffmpeg stalled for ${Math.round(stallTimeoutMs / 1000)}s. ` +
              'Export stopped to avoid hanging forever. Try fewer scenes or shorter clips.'
          );
        }, stallTimeoutMs);
      };
      hardTimer = setTimeout(() => {
        killForTimeout(
          `ffmpeg exceeded ${Math.round(hardTimeoutMs / 60000)} minutes. ` +
            'Export stopped to avoid hanging forever.'
        );
      }, hardTimeoutMs);
      bumpStallTimer();
      heartbeatTimer = setInterval(() => {
        if (settled) return;
        const pct = durationSec > 0 ? Math.max(1, Math.min(99, lastPct > 0 ? lastPct : 1)) : 1;
        const heartbeatMsg = messagePrefix
          ? `${messagePrefix} (still working...)`
          : 'Rendering timeline... still working...';
        sendExportProgress(pct, heartbeatMsg, phase);
      }, 12000);
      proc.stderr.on('data', (data) => {
        if (isExportCancelled()) {
          killForTimeout('Export reset by user.');
          return;
        }
        const text = data.toString();
        err += text;
        bumpStallTimer();
        if (durationSec > 0) {
          const t = parseFfmpegProgressSec(text);
          if (t !== null) {
            const pct = Math.max(0, Math.min(99, Math.round((t / durationSec) * 100)));
            if (pct > lastPct) {
              lastPct = pct;
              sendExportProgress(
                pct,
                messagePrefix || `Rendering timeline... ${pct}%`,
                phase
              );
            }
          }
        }
      });
      proc.on('error', (error) => {
        if (settled) return;
        settled = true;
        clearTimers();
        reject(error);
      });
      proc.on('close', (code) => {
        if (settled) return;
        settled = true;
        clearTimers();
        if (code === 0) {
          if (durationSec > 0) {
            sendExportProgress(100, messagePrefix || 'Render complete.', phase);
          }
          resolve();
        }
        else reject(new Error(err || `ffmpeg exited with ${code}`));
      });
    });

  const isHttpUrl = (value = '') => /^https?:\/\//i.test(String(value || '').trim());
  const remoteAssetCache = new Map();
  let remoteAssetRoot = '';
  const ensureRemoteAssetRoot = () => {
    if (remoteAssetRoot) return remoteAssetRoot;
    remoteAssetRoot = fs.mkdtempSync(path.join(app.getPath('temp'), 'tiktok-export-remote-'));
    return remoteAssetRoot;
  };
  const inferRemoteExt = (url = '', fallback = '.bin') => {
    try {
      const parsed = new URL(url);
      const ext = path.extname(parsed.pathname || '').toLowerCase();
      if (ext && ext.length <= 8) return ext;
    } catch (_err) {}
    return fallback;
  };
  const cacheRemoteAsset = async (url = '', label = 'asset') => {
    const raw = String(url || '').trim();
    if (!isHttpUrl(raw)) return raw;
    if (remoteAssetCache.has(raw)) return remoteAssetCache.get(raw);
    ensureExportNotCancelled();
    const tmpRoot = ensureRemoteAssetRoot();
    const ext = inferRemoteExt(raw, '.bin');
    const outPath = path.join(tmpRoot, `${label}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}${ext}`);
    const controller = new AbortController();
    exportCtx.fetchControllers.add(controller);
    const timeout = setTimeout(() => controller.abort(), 180000);
    try {
      const res = await fetch(raw, { redirect: 'follow', signal: controller.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      ensureExportNotCancelled();
      const buf = Buffer.from(await res.arrayBuffer());
      ensureExportNotCancelled();
      fs.writeFileSync(outPath, buf);
      remoteAssetCache.set(raw, outPath);
      return outPath;
    } finally {
      exportCtx.fetchControllers.delete(controller);
      clearTimeout(timeout);
    }
  };
  const localizeRemoteSceneMedia = async () => {
    const urls = new Set();
    scenes.forEach((scene) => {
      const clipUrl = String(scene?.clip?.url || '').trim();
      const overlayUrl = String(scene?.overlay_url || '').trim();
      const avatarAudioUrl = String(scene?.avatar_audio_url || '').trim();
      if (isHttpUrl(clipUrl)) urls.add(clipUrl);
      if (isHttpUrl(overlayUrl)) urls.add(overlayUrl);
      if (isHttpUrl(avatarAudioUrl)) urls.add(avatarAudioUrl);
    });
    const total = urls.size;
    if (!total) return;
    sendExportProgress(1, `Caching ${total} remote media file(s)...`, progressTag);
    let done = 0;
    const map = new Map();
    for (const url of urls) {
      ensureExportNotCancelled();
      const local = await cacheRemoteAsset(url, 'media');
      map.set(url, local);
      done += 1;
      const pct = Math.min(12, Math.max(1, Math.round((done / total) * 12)));
      sendExportProgress(pct, `Caching media ${done}/${total}...`, progressTag);
    }
    scenes.forEach((scene) => {
      const clipUrl = String(scene?.clip?.url || '').trim();
      const overlayUrl = String(scene?.overlay_url || '').trim();
      const avatarAudioUrl = String(scene?.avatar_audio_url || '').trim();
      if (clipUrl && map.has(clipUrl) && scene.clip) {
        scene.clip = { ...scene.clip, url: map.get(clipUrl) };
      }
      if (overlayUrl && map.has(overlayUrl)) {
        scene.overlay_url = map.get(overlayUrl);
      }
      if (avatarAudioUrl && map.has(avatarAudioUrl)) {
        scene.avatar_audio_url = map.get(avatarAudioUrl);
      }
    });
  };
  try {
    await localizeRemoteSceneMedia();
  } catch (err) {
    return {
      success: false,
      error: `Failed to cache remote media before export: ${err.message || 'unknown error'}`
    };
  }

  const hasOverlay = scenes.some((scene) => scene.overlay_url);
  const preferAvatarClipAudio = audioMode === 'avatar_clips';
  const runFallback = async (initialErr) => {
    const tmpRoot = fs.mkdtempSync(path.join(app.getPath('temp'), 'tiktok-export-'));
    const segments = [];
    const tryEncoders = ['libx264', 'libopenh264', 'mpeg4'];
    let encoder = '';
    let lastError = initialErr;
    const getSceneDurationSecSafe = (scene = {}) => {
      const rawDuration =
        Number(scene.duration_sec || 0) || (Number(scene.end_ms || 0) - Number(scene.start_ms || 0)) / 1000;
      const safeRaw = Number.isFinite(rawDuration) ? rawDuration : 0;
      const capped = Math.max(0.2, Math.min(20, safeRaw || 0.2));
      return capped;
    };
    const encodeSegment = async (scene, idx, enc) => {
      const duration = getSceneDurationSecSafe(scene);
      const durationStr = duration.toFixed(3);
      const segmentPath = path.join(tmpRoot, `segment-${idx}.mp4`);
      const segmentArgs = [];
      if (scene.clip?.type === 'image') {
        segmentArgs.push('-loop', '1', '-t', durationStr, '-i', scene.clip.url);
      } else {
        segmentArgs.push('-i', scene.clip.url);
      }
      const overlayUrl = scene.overlay_url || '';
      if (overlayUrl) {
        const isImage = /\.(png|jpe?g|webp)$/i.test(String(overlayUrl).split('?')[0] || '');
        if (isImage) {
          segmentArgs.push('-loop', '1', '-t', durationStr, '-i', overlayUrl);
        } else {
          segmentArgs.push('-stream_loop', '-1', '-i', overlayUrl);
        }
      }
      const overlayScale = Number(scene.overlay_scale || payload.overlay_scale || 0.28);
      const overlayMargin = Number(scene.overlay_margin || payload.overlay_margin || 0.04);
      const overlayW = Math.max(80, Math.round(width * overlayScale));
      const overlayMarginPx = Math.max(8, Math.round(width * overlayMargin));
      const baseFilter = `scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},setsar=1`;
      let vf = baseFilter;
      if (overlayUrl) {
        const overlayMode = scene.overlay_mode || payload.overlay_mode || 'screen';
        if (overlayMode === 'greenscreen' || overlayMode === 'greenscreen_black') {
          const clamp = (value, min, max) => Math.max(min, Math.min(max, Number(value)));
          const greenSimilarity = clamp(
            scene.overlay_green_similarity ?? payload.overlay_green_similarity ?? 0.24,
            0.05,
            0.45
          );
          const greenBlend = clamp(
            scene.overlay_green_blend ?? payload.overlay_green_blend ?? 0.06,
            0,
            0.2
          );
          const blackSimilarity = clamp(
            scene.overlay_black_similarity ?? payload.overlay_black_similarity ?? 0.04,
            0.005,
            0.2
          );
          const blackBlend = clamp(
            scene.overlay_black_blend ?? payload.overlay_black_blend ?? 0.01,
            0,
            0.12
          );
          const keyFilter =
            overlayMode === 'greenscreen_black'
              ? `colorkey=0x00FF00:${greenSimilarity.toFixed(3)}:${greenBlend.toFixed(3)},colorkey=0x000000:${blackSimilarity.toFixed(3)}:${blackBlend.toFixed(3)}`
              : `colorkey=0x00FF00:${greenSimilarity.toFixed(3)}:${greenBlend.toFixed(3)}`;
          vf =
            `[0:v]${baseFilter}[base];` +
            `[1:v]scale=${overlayW}:-1,format=rgba,${keyFilter}[ovl];` +
            `[base][ovl]overlay=${overlayMarginPx}:${height}-overlay_h-${overlayMarginPx}:format=auto[v]`;
        } else {
          const blendMode = overlayMode === 'screen' ? 'screen' : 'normal';
          vf =
            `[0:v]${baseFilter}[base];` +
            `[1:v]scale=${overlayW}:-1,format=rgba[ovl];` +
            `[ovl]pad=${width}:${height}:${overlayMarginPx}:${height}-overlay_h-${overlayMarginPx}:color=0x00000000[ovlp];` +
            `[base][ovlp]blend=all_mode=${blendMode}:all_opacity=1[v]`;
        }
      }
      if (overlayUrl) {
        segmentArgs.push('-filter_complex', vf, '-map', '[v]');
      } else {
        segmentArgs.push('-vf', vf);
      }
      segmentArgs.push(
        '-r',
        String(fps),
        '-fps_mode',
        'cfr',
        '-threads',
        '0',
        '-c:v',
        enc
      );
      if (enc === 'libx264') {
        segmentArgs.push(
          '-preset',
          'ultrafast',
          '-tune',
          'zerolatency',
          '-g',
          String(Math.max(24, Math.round(fps * 2))),
          '-keyint_min',
          String(Math.max(12, Math.round(fps))),
          '-sc_threshold',
          '0'
        );
      } else if (enc === 'mpeg4') {
        segmentArgs.push('-q:v', '7');
      }
      segmentArgs.push(
        '-b:v',
        '3M',
        '-maxrate',
        '3M',
        '-bufsize',
        '6M',
        '-an',
        '-pix_fmt',
        'yuv420p',
        '-t',
        durationStr,
        '-movflags',
        '+faststart',
        '-y',
        segmentPath
      );
      const segmentHardTimeoutMs = Math.max(30000, Math.min(90000, Math.round(15000 + duration * 3000)));
      await runFfmpeg(segmentArgs, {
        phase: progressTag,
        messagePrefix: `Rendering segment ${idx + 1}/${scenes.length}...`,
        stallTimeoutMs: 25000,
        hardTimeoutMs: segmentHardTimeoutMs
      });
      return segmentPath;
    };
    const encodePlaceholderSegment = async (scene, idx, enc) => {
      const duration = getSceneDurationSecSafe(scene);
      const durationStr = duration.toFixed(3);
      const segmentPath = path.join(tmpRoot, `segment-${idx}-placeholder.mp4`);
      const args = [
        '-f',
        'lavfi',
        '-i',
        `color=c=black:s=${width}x${height}:d=${durationStr}`,
        '-r',
        String(fps),
        '-fps_mode',
        'cfr',
        '-threads',
        '0',
        '-c:v',
        enc
      ];
      if (enc === 'libx264') {
        args.push(
          '-preset',
          'ultrafast',
          '-tune',
          'zerolatency',
          '-g',
          String(Math.max(24, Math.round(fps * 2))),
          '-keyint_min',
          String(Math.max(12, Math.round(fps))),
          '-sc_threshold',
          '0'
        );
      } else if (enc === 'mpeg4') {
        args.push('-q:v', '8');
      }
      args.push('-pix_fmt', 'yuv420p', '-t', durationStr, '-movflags', '+faststart', '-y', segmentPath);
      await runFfmpeg(args, {
        phase: progressTag,
        messagePrefix: `Recovering segment ${idx + 1}/${scenes.length}...`,
        stallTimeoutMs: 20000,
        hardTimeoutMs: 60000
      });
      return segmentPath;
    };
    const encodeSilenceAudioSegment = async (idx, durationSec = 0.2) => {
      const duration = Math.max(0.2, Number(durationSec) || 0.2);
      const durationStr = duration.toFixed(3);
      const outPath = path.join(tmpRoot, `audio-${idx}-silence.m4a`);
      await runFfmpeg(
        [
          '-f',
          'lavfi',
          '-i',
          'anullsrc=channel_layout=stereo:sample_rate=48000',
          '-t',
          durationStr,
          '-c:a',
          'aac',
          '-b:a',
          '192k',
          '-ar',
          '48000',
          '-ac',
          '2',
          '-y',
          outPath
        ],
        {
          phase: progressTag,
          messagePrefix: `Preparing audio ${idx + 1}/${scenes.length}...`,
          stallTimeoutMs: 20000,
          hardTimeoutMs: 60000
        }
      );
      return outPath;
    };
    const encodeAvatarAudioSegment = async (scene, idx) => {
      const duration = getSceneDurationSecSafe(scene);
      const durationStr = duration.toFixed(3);
      const outPath = path.join(tmpRoot, `audio-${idx}.m4a`);
      const rawAudioSource = String(
        scene?.avatar_audio_url ||
          scene?.overlay_url ||
          (String(scene?.clip?.provider || '').toLowerCase() === 'avatar' ? scene?.clip?.url || '' : '')
      ).trim();
      const source = rawAudioSource;
      if (!source || /\.(png|jpe?g|webp)(\?|$)/i.test(source)) {
        return encodeSilenceAudioSegment(idx, duration);
      }
      try {
        await runFfmpeg(
          [
            '-i',
            source,
            '-vn',
            '-af',
            `apad,atrim=0:${durationStr},asetpts=N/SR/TB`,
            '-c:a',
            'aac',
            '-b:a',
            '192k',
            '-ar',
            '48000',
            '-ac',
            '2',
            '-y',
            outPath
          ],
          {
            phase: progressTag,
            messagePrefix: `Extracting Step 7 audio ${idx + 1}/${scenes.length}...`,
            stallTimeoutMs: 25000,
            hardTimeoutMs: 90000
          }
        );
        return outPath;
      } catch (_err) {
        return encodeSilenceAudioSegment(idx, duration);
      }
    };
    const buildAvatarClipAudioTrack = async () => {
      const audioSegments = [];
      for (let i = 0; i < scenes.length; i += 1) {
        ensureExportNotCancelled();
        const segPath = await encodeAvatarAudioSegment(scenes[i], i);
        audioSegments.push(segPath);
      }
      if (!audioSegments.length) return '';
      const listPath = path.join(tmpRoot, 'audio-concat.txt');
      fs.writeFileSync(listPath, audioSegments.map((p) => `file '${p.replace(/'/g, "'\\''")}'`).join('\n'));
      const trackPath = path.join(tmpRoot, 'avatar-audio-track.m4a');
      await runFfmpeg(
        [
          '-f',
          'concat',
          '-safe',
          '0',
          '-i',
          listPath,
          '-c:a',
          'aac',
          '-b:a',
          '192k',
          '-ar',
          '48000',
          '-ac',
          '2',
          '-af',
          'aresample=async=1:first_pts=0',
          '-y',
          trackPath
        ],
        {
          phase: progressTag,
          messagePrefix: 'Combining Step 7 clip audio...',
          stallTimeoutMs: 30000,
          hardTimeoutMs: 120000
        }
      );
      return trackPath;
    };

    try {
      for (const enc of tryEncoders) {
        try {
          sendExportProgress(13, `Checking encoder (${enc})...`, progressTag);
          await probeVideoEncoder(enc);
          encoder = enc;
          break;
        } catch (segmentErr) {
          lastError = segmentErr;
        }
      }
      if (!encoder) throw lastError;
      sendExportProgress(14, `Rendering segment 1/${scenes.length}...`, progressTag);
      try {
        const firstSeg = await encodeSegment(scenes[0], 0, encoder);
        segments.push(firstSeg);
      } catch (firstErr) {
        lastError = firstErr;
        sendExportProgress(14, `Scene 1 failed. Using placeholder segment...`, progressTag);
        const placeholder = await encodePlaceholderSegment(scenes[0], 0, encoder);
        segments.push(placeholder);
      }
      for (let i = 1; i < scenes.length; i += 1) {
        const startedPct = Math.min(94, Math.max(14, Math.round((i / Math.max(1, scenes.length)) * 92)));
        sendExportProgress(startedPct, `Rendering segment ${i + 1}/${scenes.length}...`, progressTag);
        try {
          const segPath = await encodeSegment(scenes[i], i, encoder);
          segments.push(segPath);
        } catch (segErr) {
          lastError = segErr;
          sendExportProgress(startedPct, `Scene ${i + 1} failed. Using placeholder segment...`, progressTag);
          const placeholder = await encodePlaceholderSegment(scenes[i], i, encoder);
          segments.push(placeholder);
        }
        const pct = Math.min(94, Math.round((segments.length / Math.max(1, scenes.length)) * 92));
        sendExportProgress(pct, `Rendered segment ${segments.length}/${scenes.length}.`, progressTag);
      }
      const listPath = path.join(tmpRoot, 'concat.txt');
      fs.writeFileSync(listPath, segments.map((p) => `file '${p.replace(/'/g, "'\\''")}'`).join('\n'));
      const concatOutput = path.join(tmpRoot, 'concat.mp4');
      sendExportProgress(95, 'Combining rendered segments...', progressTag);
      const concatArgs = [
        '-f',
        'concat',
        '-safe',
        '0',
        '-fflags',
        '+genpts',
        '-i',
        listPath,
        '-vf',
        `fps=${fps},format=yuv420p`,
        '-r',
        String(fps),
        '-fps_mode',
        'cfr',
        '-c:v',
        encoder,
        '-b:v',
        '3M',
        '-maxrate',
        '3M',
        '-bufsize',
        '6M',
        '-an'
      ];
      if (encoder === 'libx264') {
        concatArgs.push(
          '-preset',
          'ultrafast',
          '-tune',
          'zerolatency',
          '-g',
          String(Math.max(24, Math.round(fps * 2))),
          '-keyint_min',
          String(Math.max(12, Math.round(fps))),
          '-sc_threshold',
          '0'
        );
      } else if (encoder === 'mpeg4') {
        concatArgs.push('-q:v', '7');
      }
      concatArgs.push('-movflags', '+faststart', '-y', concatOutput);
      await runFfmpeg(concatArgs, {
        phase: progressTag,
        messagePrefix: 'Combining rendered segments...',
        stallTimeoutMs: 35000,
        hardTimeoutMs: 180000
      });
      let selectedAudioPath = '';
      if (preferAvatarClipAudio) {
        sendExportProgress(97, 'Building soundtrack from Step 7 avatar clips...', progressTag);
        selectedAudioPath = await buildAvatarClipAudioTrack();
      }
      if (!selectedAudioPath && audioPath && fs.existsSync(audioPath)) {
        selectedAudioPath = audioPath;
      }
      if (selectedAudioPath) {
        sendExportProgress(98, 'Muxing final audio track...', progressTag);
        await runFfmpeg([
          '-i',
          concatOutput,
          '-i',
          selectedAudioPath,
          '-map',
          '0:v:0',
          '-map',
          '1:a:0',
          '-c:v',
          'copy',
          '-c:a',
          'aac',
          '-b:a',
          '192k',
          '-af',
          'aresample=async=1:first_pts=0',
          '-movflags',
          '+faststart',
          '-y',
          outputPath
        ]);
      } else {
        fs.copyFileSync(concatOutput, outputPath);
      }
      await ensureVoiceoverAudioInOutput(selectedAudioPath || audioPath);
      sendExportProgress(100, 'Timeline export complete.', progressTag);
      return {
        success: true,
        path: outputPath,
        fallback: true,
        debug: {
          ...exportDebugInfo,
          hasOverlay,
          selectedAudioPath: selectedAudioPath || ''
        }
      };
    } catch (fallbackErr) {
      return { success: false, error: fallbackErr.message || 'ffmpeg failed' };
    }
  };

  if (hasOverlay || preferAvatarClipAudio) {
    return runFallback(new Error(hasOverlay ? 'Overlay enabled' : 'Avatar clip audio mode'));
  }

  const args = [];
  const filterParts = [];
  const concatInputs = [];
  let primaryEncoder = 'libx264';
  try {
    primaryEncoder = await selectPrimaryEncoder();
  } catch (encoderErr) {
    return runFallback(encoderErr);
  }
  scenes.forEach((scene, idx) => {
    const duration =
      Math.max(0.2, Number(scene.duration_sec || 0) || (Number(scene.end_ms || 0) - Number(scene.start_ms || 0)) / 1000);
    const durationStr = duration.toFixed(3);
    if (scene.clip?.type === 'image') {
      args.push('-loop', '1', '-t', durationStr, '-i', scene.clip.url);
      filterParts.push(
        `[${idx}:v]scale=${width}:${height}:force_original_aspect_ratio=increase,` +
          `crop=${width}:${height},setsar=1[v${idx}]`
      );
    } else {
      args.push('-i', scene.clip.url);
      filterParts.push(
        `[${idx}:v]trim=0:${durationStr},setpts=PTS-STARTPTS,` +
          `scale=${width}:${height}:force_original_aspect_ratio=increase,` +
          `crop=${width}:${height},setsar=1[v${idx}]`
      );
    }
    concatInputs.push(`[v${idx}]`);
  });

  const videoCount = scenes.length;
  if (audioPath) {
    args.push('-i', audioPath);
  }

  const filterComplex = `${filterParts.join(';')};${concatInputs.join('')}concat=n=${videoCount}:v=1:a=0[vout]`;
  args.push('-filter_complex', filterComplex, '-map', '[vout]');
  if (audioPath) {
    args.push('-map', `${videoCount}:a`);
  }
  args.push(
    '-r',
    String(fps),
    '-fps_mode',
    'cfr',
    '-c:v',
    primaryEncoder,
    '-b:v',
    '3M',
    '-maxrate',
    '3M',
    '-bufsize',
    '6M',
    '-pix_fmt',
    'yuv420p'
  );
  if (primaryEncoder === 'libx264') {
    args.push('-preset', 'ultrafast', '-tune', 'zerolatency');
  } else if (primaryEncoder === 'mpeg4') {
    args.push('-q:v', '7');
  }
  if (audioPath) {
    args.push('-c:a', 'aac', '-b:a', '192k', '-af', 'aresample=async=1:first_pts=0');
  }
  args.push('-movflags', '+faststart');
  args.push('-y', outputPath);

  try {
    sendExportProgress(1, 'Starting ffmpeg render...', progressTag);
    await runFfmpeg(args, {
      durationSec: totalDurationSec,
      phase: progressTag,
      messagePrefix: 'Rendering timeline...'
    });
    await ensureVoiceoverAudioInOutput();
    sendExportProgress(100, 'Timeline export complete.', progressTag);
    return {
      success: true,
      path: outputPath,
      debug: {
        ...exportDebugInfo,
        hasOverlay: false
      }
    };
  } catch (err) {
    return runFallback(err);
  }
  } finally {
    if (runIdForCancel) {
      activeTikTokExports.delete(runIdForCancel);
    }
  }
});
