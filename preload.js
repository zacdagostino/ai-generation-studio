const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  uploadFiles: (filePaths, options = {}) => ipcRenderer.invoke('upload-files', { filePaths, ...options }),
  uploadBlobs: (files, options = {}) => ipcRenderer.invoke('upload-blobs', { files, ...options }),
  deleteFiles: (paths) => ipcRenderer.invoke('delete-files', paths),
  sendLog: (level, message) => ipcRenderer.send('renderer-log', { level, message }),
  persistBlobFile: (payload) => ipcRenderer.invoke('persist-blob-file', payload),
  recoverLocalStorageArraySnapshot: (storageKey) => ipcRenderer.invoke('recover-localstorage-array-snapshot', storageKey),
  loadSavedProjectsSync: () => ipcRenderer.sendSync('load-saved-projects-sync'),
  persistSavedProjectsSync: (projects = []) => ipcRenderer.sendSync('persist-saved-projects-sync', projects),
  readLocalFile: (filePath) => ipcRenderer.invoke('read-local-file', filePath),
  pathExists: (filePath) => ipcRenderer.invoke('path-exists', filePath),
  cacheRemoteFile: (payload) => ipcRenderer.invoke('cache-remote-file', payload),
  revealInFolder: (filePath) => ipcRenderer.invoke('reveal-in-folder', filePath),
  extractVideoFrames: (payload) => ipcRenderer.invoke('extract-video-frames', payload),
  saveRemoteFile: (payload) => ipcRenderer.invoke('save-remote-file', payload),
  selectImageSaveTarget: (payload) => ipcRenderer.invoke('select-image-save-target', payload),
  getImageSequenceNextIndex: (payload) => ipcRenderer.invoke('get-image-sequence-next-index', payload),
  saveImageFile: (payload) => ipcRenderer.invoke('save-image-file', payload),
  connectKieLogsAccount: () => ipcRenderer.invoke('connect-kie-logs-account'),
  disconnectKieLogsAccount: () => ipcRenderer.invoke('disconnect-kie-logs-account'),
  queryKieLogsPage: (payload) => ipcRenderer.invoke('query-kie-logs-page', payload),
  onKieLogsConnectProgress: (callback) => {
    if (typeof callback !== 'function') return () => {};
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('kie-logs-connect-progress', listener);
    return () => {
      try {
        ipcRenderer.removeListener('kie-logs-connect-progress', listener);
      } catch (_err) {}
    };
  },
  onKieLogsScrapeProgress: (callback) => {
    if (typeof callback !== 'function') return () => {};
    const listener = (_event, data) => callback(data);
    ipcRenderer.on('kie-logs-scrape-progress', listener);
    return () => {
      try {
        ipcRenderer.removeListener('kie-logs-scrape-progress', listener);
      } catch (_err) {}
    };
  },
  downloadYoutubeClip: (payload) => ipcRenderer.invoke('download-youtube-clip', payload),
  onYoutubeDownloadProgress: (callback) =>
    ipcRenderer.on('youtube-download-progress', (_event, data) => callback(data)),
  onTikTokExportProgress: (callback) =>
    ipcRenderer.on('tiktok-export-progress', (_event, data) => callback(data)),
  onTikTokCaptionRenderProgress: (callback) =>
    ipcRenderer.on('tiktok-caption-render-progress', (_event, data) => callback(data)),
  onTikTokZoomRenderProgress: (callback) =>
    ipcRenderer.on('tiktok-zoom-render-progress', (_event, data) => callback(data)),
  onTikTokSfxProgress: (callback) =>
    ipcRenderer.on('tiktok-sfx-render-progress', (_event, data) => callback(data)),
  onTikTokIntroMaskProgress: (callback) =>
    ipcRenderer.on('tiktok-intro-mask-progress', (_event, data) => callback(data)),
  runLocalAlignment: (payload) => ipcRenderer.invoke('run-local-alignment', payload),
  exportTikTokTimeline: (payload) => ipcRenderer.invoke('export-tiktok-timeline', payload),
  cancelTikTokExport: (payload) => ipcRenderer.invoke('cancel-tiktok-export', payload),
  splitAudio: (payload) => ipcRenderer.invoke('split-audio', payload),
  transcribeOpenaiAudio: (payload) => ipcRenderer.invoke('transcribe-openai-audio', payload),
  previewTikTokIntroMask: (payload) => ipcRenderer.invoke('preview-tiktok-intro-mask', payload),
  renderTikTokCaptionedVideo: (payload) => ipcRenderer.invoke('render-tiktok-captioned-video', payload),
  renderTikTokIntroZoomVideo: (payload) => ipcRenderer.invoke('render-tiktok-intro-zoom-video', payload),
  renderTikTokSfxVideo: (payload) => ipcRenderer.invoke('render-tiktok-sfx-video', payload),
  combineTikTokCaptionAndIntroMask: (payload) => ipcRenderer.invoke('combine-tiktok-caption-intro-mask', payload),
  publishSocialPosts: (payload) => ipcRenderer.invoke('publish-social-posts', payload),
  runpodListPods: (payload) => ipcRenderer.invoke('runpod-list-pods', payload),
  runpodGetPod: (payload) => ipcRenderer.invoke('runpod-get-pod', payload),
  runpodProbeUrl: (payload) => ipcRenderer.invoke('runpod-probe-url', payload),
  openExternalUrl: (payload) => ipcRenderer.invoke('open-external-url', payload),
  onMenuSave: (callback) => ipcRenderer.on('menu-save', () => callback()),
  onAppCloseRequested: (callback) => ipcRenderer.on('app-close-requested', () => callback()),
  respondToAppClose: (allow) => ipcRenderer.send('app-close-response', { allow: !!allow })
});
