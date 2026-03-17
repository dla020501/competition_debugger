(() => {
  const payload = JSON.parse(document.getElementById("payload").textContent);
  const video = document.getElementById("video");
  const canvas = document.getElementById("overlay");
  const videoWrap = document.getElementById("video-wrap");
  const videoHudEl = document.getElementById("video-hud");
  const zoomSelectionBoxEl = document.getElementById("zoom-selection-box");
  const ctx = canvas.getContext("2d");

  const pred = payload.pred || {};
  let gt = payload.gt || {};
  let draftGt = { ...gt };

  const editorEl = document.getElementById("editor");
  const tEl = document.getElementById("gt_time");
  const cxEl = document.getElementById("gt_cx");
  const cyEl = document.getElementById("gt_cy");
  const typeEl = document.getElementById("gt_type");
  const noteEl = document.getElementById("gt_note");
  const saveMsg = document.getElementById("save-msg");
  const saveState = document.getElementById("save-state");

  const predText = document.getElementById("pred-text");
  const gtText = document.getElementById("gt-text");
  const metaText = document.getElementById("meta-text");
  const cursorPos = document.getElementById("cursor-pos");
  const historyBody = document.getElementById("history-body");
  const eventMarkerRailEl = document.getElementById("event-marker-rail");
  const gtPanelEl = document.getElementById("gt-panel");
  const gtPanelTitleEl = document.getElementById("gt-panel-title");
  const gtEditFieldsEl = document.getElementById("gt-edit-fields");
  const editModeEl = document.getElementById("edit-mode");
  const toggleEditModeBtn = document.getElementById("toggle-edit-mode");
  const editModeStatusEl = document.getElementById("edit-mode-status");
  const playbackTimeDisplayEl = document.getElementById("playback-time-display");
  const playbackRateButtons = Array.from(document.querySelectorAll("[data-playback-rate]"));
  const playbackRateStatusEl = document.getElementById("playback-rate-status");
  const setTimeFromVideoBtn = document.getElementById("sync-time-from-video");
  const videoStatusEl = document.getElementById("video-status");
  const openVideoDirectEl = document.getElementById("open-video-direct");
  const zoomModeEl = document.getElementById("zoom-mode");
  const toggleZoomModeBtn = document.getElementById("toggle-zoom-mode");
  const resetZoomBtn = document.getElementById("reset-zoom");
  const zoomStateEl = document.getElementById("zoom-state");

  let isDraggingGt = false;
  let isDraggingZoom = false;
  let hoverPoint = null;
  let loadTimeoutId = null;
  let zoomSelectionStart = null;
  let zoomPreviewRect = null;
  let zoomRect = null;
  let zoomTransform = { scale: 1, tx: 0, ty: 0 };
  let lastVideoHudText = "";
  const serverCanEdit = !saveMsg || !document.getElementById("save-gt")?.disabled;
  const MIN_ZOOM_RECT_SIZE = 0.05;
  const ZOOM_PADDING_FACTOR = 1.15;
  const MAX_ZOOM_SCALE = 6;
  const PLAYBACK_RATE_STORAGE_KEY = "video_playback_rate";

  function num(v, fallback = null) {
    const x = Number(v);
    return Number.isFinite(x) ? x : fallback;
  }

  function clamp(v, min, max) {
    return Math.max(min, Math.min(max, v));
  }

  function formatPlaybackRate(rate) {
    return `${Number(rate).toFixed(2).replace(/\.?0+$/, "")}x`;
  }

  function syncPlaybackRateUI(rate) {
    const normalized = String(rate);
    playbackRateButtons.forEach((button) => {
      const active = button.dataset.playbackRate === normalized;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
    if (playbackRateStatusEl) {
      playbackRateStatusEl.textContent = `현재 ${formatPlaybackRate(rate)}`;
    }
  }

  function applyPlaybackRate(nextRate, { persist = true } = {}) {
    const rate = clamp(num(nextRate, 1), 0.25, 2);
    video.defaultPlaybackRate = rate;
    video.playbackRate = rate;
    syncPlaybackRateUI(rate);
    if (persist) {
      localStorage.setItem(PLAYBACK_RATE_STORAGE_KEY, String(rate));
    }
  }

  function loadSavedPlaybackRate() {
    const saved = localStorage.getItem(PLAYBACK_RATE_STORAGE_KEY);
    if (!saved) {
      syncPlaybackRateUI(1);
      return;
    }
    applyPlaybackRate(saved, { persist: false });
  }

  function buildEventMarkers() {
    if (!eventMarkerRailEl) {
      return;
    }
    eventMarkerRailEl.innerHTML = "";

    const duration = Number.isFinite(video.duration) ? video.duration : null;
    if (!duration || duration <= 0) {
      eventMarkerRailEl.innerHTML = '<div class="event-marker-empty">메타데이터 로딩 후 Pred/GT 시점이 표시됩니다.</div>';
      return;
    }

    const validMarkers = [
      { key: "pred", label: "Pred", cssClass: "pred", time: pred.accident_time },
      { key: "gt", label: "GT", cssClass: "gt", time: gt.accident_time },
    ]
      .map((m) => ({ ...m, t: num(m.time, null) }))
      .filter((m) => m.t != null);

    if (!validMarkers.length) {
      eventMarkerRailEl.innerHTML = '<div class="event-marker-empty">표시할 Pred/GT 시점이 없습니다.</div>';
      return;
    }

    validMarkers.forEach((m) => {
      const ratio = clamp((m.t || 0) / duration, 0, 1);
      const marker = document.createElement("button");
      marker.type = "button";
      marker.className = `event-marker-item ${m.cssClass}`;
      marker.style.left = `${(ratio * 100).toFixed(2)}%`;
      marker.innerHTML = `<span class="event-marker-stick"></span><span class="event-marker-label">${m.label} ${Number(m.t).toFixed(2)}s</span>`;
      marker.title = `${m.label} 시점으로 이동 (${Number(m.t).toFixed(2)}s)`;
      marker.addEventListener("click", () => {
        video.currentTime = Math.max(0, Math.min(duration, Number(m.t)));
      });
      eventMarkerRailEl.appendChild(marker);
    });
  }

  function syncFormFromGt() {
    tEl.value = gt.accident_time ?? "";
    cxEl.value = gt.center_x ?? "";
    cyEl.value = gt.center_y ?? "";
    typeEl.value = gt.type ?? "";
    noteEl.value = gt.note ?? "";
    draftGt = { ...gt };
  }

  function getRenderGt() {
    return (editModeEl && editModeEl.checked) ? draftGt : gt;
  }

  function isEditEnabled() {
    return !!(serverCanEdit && editModeEl && editModeEl.checked);
  }

  function isZoomSelectEnabled() {
    return !!(zoomModeEl && zoomModeEl.checked);
  }

  function rectFromPoints(a, b) {
    const x1 = Math.min(a.x, b.x);
    const y1 = Math.min(a.y, b.y);
    const x2 = Math.max(a.x, b.x);
    const y2 = Math.max(a.y, b.y);
    return {
      x: clamp(x1, 0, 1),
      y: clamp(y1, 0, 1),
      w: clamp(x2 - x1, 0, 1),
      h: clamp(y2 - y1, 0, 1),
    };
  }

  function expandRect(rect, factor) {
    if (!rect) {
      return null;
    }
    const cx = rect.x + rect.w / 2;
    const cy = rect.y + rect.h / 2;
    const w = clamp(rect.w * factor, MIN_ZOOM_RECT_SIZE, 1);
    const h = clamp(rect.h * factor, MIN_ZOOM_RECT_SIZE, 1);
    return {
      x: clamp(cx - w / 2, 0, 1 - w),
      y: clamp(cy - h / 2, 0, 1 - h),
      w,
      h,
    };
  }

  function applyZoomTransform() {
    const baseWidth = canvas.width || Math.max(1, Math.round(videoWrap?.clientWidth || video.clientWidth || 1));
    const baseHeight = canvas.height || Math.max(1, Math.round(videoWrap?.clientHeight || video.clientHeight || 1));
    if (!zoomRect) {
      zoomTransform = { scale: 1, tx: 0, ty: 0 };
    } else {
      const padded = expandRect(zoomRect, ZOOM_PADDING_FACTOR) || zoomRect;
      const scale = clamp(Math.min(1 / Math.max(padded.w, 1e-6), 1 / Math.max(padded.h, 1e-6)), 1, MAX_ZOOM_SCALE);
      const centerX = (padded.x + padded.w / 2) * baseWidth;
      const centerY = (padded.y + padded.h / 2) * baseHeight;
      const minTx = baseWidth - baseWidth * scale;
      const minTy = baseHeight - baseHeight * scale;
      zoomTransform = {
        scale,
        tx: clamp(baseWidth / 2 - scale * centerX, minTx, 0),
        ty: clamp(baseHeight / 2 - scale * centerY, minTy, 0),
      };
    }
    const { scale, tx, ty } = zoomTransform;
    const matrix = `matrix(${scale}, 0, 0, ${scale}, ${tx}, ${ty})`;
    video.style.transform = matrix;
    canvas.style.transform = matrix;
    syncZoomSelectionBox();
    if (resetZoomBtn) {
      resetZoomBtn.disabled = !zoomRect;
    }
  }

  function setZoomRect(nextRect) {
    zoomRect = nextRect;
    applyZoomTransform();
  }

  function clearZoomPreview() {
    zoomSelectionStart = null;
    zoomPreviewRect = null;
    isDraggingZoom = false;
    syncZoomSelectionBox();
  }

  function resetZoom() {
    setZoomRect(null);
    clearZoomPreview();
    if (zoomModeEl) {
      zoomModeEl.checked = false;
    }
    applyEditModeUI();
    showVideoSuccess("확대 해제");
  }

  function updateVideoHud(text) {
    if (!videoHudEl) {
      return;
    }
    const nextText = text || "";
    videoHudEl.hidden = !nextText;
    if (nextText === lastVideoHudText) {
      return;
    }
    lastVideoHudText = nextText;
    videoHudEl.textContent = nextText;
  }

  function syncZoomSelectionBox() {
    if (!zoomSelectionBoxEl) {
      return;
    }
    if (!zoomPreviewRect || !isZoomSelectEnabled()) {
      zoomSelectionBoxEl.hidden = true;
      return;
    }

    const baseWidth = Math.max(canvas.width, 1);
    const baseHeight = Math.max(canvas.height, 1);
    const { scale, tx, ty } = zoomTransform;
    const left = zoomPreviewRect.x * baseWidth * scale + tx;
    const top = zoomPreviewRect.y * baseHeight * scale + ty;
    const width = zoomPreviewRect.w * baseWidth * scale;
    const height = zoomPreviewRect.h * baseHeight * scale;

    zoomSelectionBoxEl.style.left = `${left}px`;
    zoomSelectionBoxEl.style.top = `${top}px`;
    zoomSelectionBoxEl.style.width = `${width}px`;
    zoomSelectionBoxEl.style.height = `${height}px`;
    zoomSelectionBoxEl.hidden = width <= 0 || height <= 0;
  }

  function applyEditModeUI() {
    const enabled = isEditEnabled();
    const zoomSelecting = isZoomSelectEnabled();
    if (editModeEl && editModeEl.checked && !serverCanEdit) {
      editModeEl.checked = false;
    }
    canvas.style.pointerEvents = (enabled || zoomSelecting) ? "auto" : "none";
    canvas.style.cursor = (enabled || zoomSelecting) ? "crosshair" : "default";
    if (toggleEditModeBtn) {
      toggleEditModeBtn.textContent = enabled ? "편집 모드: ON" : "편집 모드: OFF";
      toggleEditModeBtn.disabled = !serverCanEdit;
    }
    if (toggleZoomModeBtn) {
      toggleZoomModeBtn.textContent = zoomSelecting ? "영역 확대 선택: ON" : "영역 확대 선택: OFF";
    }
    if (editModeStatusEl) {
      if (!serverCanEdit) {
        editModeStatusEl.textContent = "이 소스는 읽기 전용입니다.";
      } else if (zoomSelecting) {
        editModeStatusEl.textContent = "확대 영역 선택 모드";
      } else {
        editModeStatusEl.textContent = enabled ? "드래그 편집 활성화" : "재생/탐색 모드";
      }
    }
    if (gtPanelTitleEl) {
      if (!serverCanEdit) {
        gtPanelTitleEl.textContent = "Expected GT (읽기 전용)";
      } else {
        gtPanelTitleEl.textContent = enabled ? "Expected GT 편집" : "Expected GT (읽기 전용)";
      }
    }
    if (gtEditFieldsEl) {
      gtEditFieldsEl.disabled = !enabled;
    }
    if (gtPanelEl) {
      gtPanelEl.classList.toggle("panel-disabled", !enabled);
    }
    if (cursorPos) {
      if (zoomSelecting) {
        cursorPos.textContent = "확대할 영역을 드래그해서 지정하세요.";
      } else if (enabled) {
        cursorPos.textContent = "편집 모드: 마우스를 움직이면 GT 마커가 따라오고, 드래그하면 위치를 반영합니다.";
      } else {
        cursorPos.textContent = "오버레이 보기 모드 (Pred/GT)";
      }
    }
    if (saveState) {
      if (!enabled) {
        saveState.textContent = saveState.textContent || "읽기 전용 보기 모드";
      } else {
        saveState.textContent = "편집 중: 드래그 후 저장을 눌러 확정하세요.";
      }
    }
    if (zoomStateEl) {
      if (zoomSelecting) {
        zoomStateEl.textContent = zoomRect
          ? "드래그해서 새 확대 영역을 지정하세요."
          : "드래그해서 확대할 영역을 지정하세요.";
      } else if (zoomRect) {
        zoomStateEl.textContent = `확대 적용 중 (${zoomTransform.scale.toFixed(2)}x). 확대 해제로 전체 화면으로 복귀합니다.`;
      } else {
        zoomStateEl.textContent = "전체 화면";
      }
    }
    hoverPoint = null;
  }

  function syncDraftFromForm() {
    draftGt = {
      ...draftGt,
      video_path: payload.video_path,
      accident_time: num(tEl.value),
      center_x: num(cxEl.value),
      center_y: num(cyEl.value),
      type: (typeEl.value || "").trim() || null,
      note: (noteEl.value || "").trim() || null,
      updated_by: gt.updated_by,
      updated_at: gt.updated_at,
    };
  }

  function drawMarker(xn, yn, color, label) {
    if (xn == null || yn == null) {
      return;
    }
    const w = canvas.width;
    const h = canvas.height;
    const x = Math.round(xn * w);
    const y = Math.round(yn * h);
    const r = Math.max(12, Math.round(Math.min(w, h) * 0.03));

    ctx.strokeStyle = color;
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.stroke();

    ctx.beginPath();
    ctx.moveTo(x - r, y);
    ctx.lineTo(x + r, y);
    ctx.moveTo(x, y - r);
    ctx.lineTo(x, y + r);
    ctx.stroke();

    ctx.fillStyle = "rgba(0,0,0,0.6)";
    ctx.fillRect(Math.max(0, x - 38), Math.max(0, y - r - 30), 76, 20);
    ctx.fillStyle = color;
    ctx.font = "bold 12px sans-serif";
    ctx.fillText(label, Math.max(4, x - 32), Math.max(14, y - r - 15));
  }

  function pointFromEvent(e) {
    const rect = videoWrap.getBoundingClientRect();
    const baseWidth = Math.max(canvas.width, 1);
    const baseHeight = Math.max(canvas.height, 1);
    const px = clamp(e.clientX - rect.left, 0, rect.width);
    const py = clamp(e.clientY - rect.top, 0, rect.height);
    return {
      x: clamp((px - zoomTransform.tx) / (baseWidth * zoomTransform.scale), 0, 1),
      y: clamp((py - zoomTransform.ty) / (baseHeight * zoomTransform.scale), 0, 1),
    };
  }

  function drawInfo() {
    const t = video.currentTime || 0;
    const renderGt = getRenderGt();
    const lines = [
      `Pred: time=${(pred.accident_time ?? 0).toFixed ? pred.accident_time.toFixed(2) : pred.accident_time}, type=${pred.type ?? "-"}`,
      `GT: time=${(renderGt.accident_time ?? 0).toFixed ? renderGt.accident_time.toFixed(2) : renderGt.accident_time}, type=${renderGt.type ?? "-"}`,
    ];
    updateVideoHud(lines.join("\n"));

    if (pred.accident_time != null && Math.abs(t - pred.accident_time) < 0.2) {
      ctx.strokeStyle = "#facc15";
      ctx.lineWidth = 6;
      ctx.strokeRect(3, 3, canvas.width - 6, canvas.height - 6);
    }
    if (renderGt.accident_time != null && Math.abs(t - renderGt.accident_time) < 0.2) {
      ctx.strokeStyle = "#22c55e";
      ctx.lineWidth = 3;
      ctx.strokeRect(10, 10, canvas.width - 20, canvas.height - 20);
    }
  }

  function drawEditGuide() {
    if (!isEditEnabled() || zoomRect) {
      return;
    }
    ctx.fillStyle = "rgba(15, 23, 42, 0.35)";
    ctx.fillRect(10, canvas.height - 40, 220, 28);
    ctx.fillStyle = "#f8fafc";
    ctx.font = "12px sans-serif";
    ctx.fillText("드래그하여 GT 위치 지정", 18, canvas.height - 22);
  }

  function drawZoomGuide() {
    const rect = zoomRect;
    if (!rect || rect.w <= 0 || rect.h <= 0) {
      return;
    }

    const x = rect.x * canvas.width;
    const y = rect.y * canvas.height;
    const w = rect.w * canvas.width;
    const h = rect.h * canvas.height;

    ctx.save();
    ctx.fillStyle = "rgba(34, 211, 238, 0.00)";
    ctx.strokeStyle = "#06b6d4";
    ctx.lineWidth = 2;
    ctx.setLineDash([10, 6]);
    ctx.fillRect(x, y, w, h);
    ctx.strokeRect(x, y, w, h);
    ctx.setLineDash([]);
    ctx.fillStyle = "rgba(8, 47, 73, 0.72)";
    ctx.fillRect(x, Math.max(0, y - 24), 104, 18);
    ctx.fillStyle = "#ecfeff";
    ctx.font = "bold 11px sans-serif";
    ctx.fillText("ZOOM", x + 8, Math.max(12, y - 11));
    ctx.restore();
  }

  function renderHUDText() {
    const renderGt = getRenderGt();
    const lineA = `Pred: t=${pred.accident_time ?? "-"}, x=${pred.center_x ?? "-"}, y=${pred.center_y ?? "-"}, type=${pred.type ?? "-"}`;
    predText.textContent = lineA;
    gtText.textContent = `GT: t=${renderGt.accident_time ?? "-"}, x=${renderGt.center_x ?? "-"}, y=${renderGt.center_y ?? "-"}, type=${renderGt.type ?? "-"}, by=${gt.updated_by ?? "-"}`;
    const m = payload.meta || {};
    metaText.textContent = `duration=${m.duration ?? "-"}s, quality=${m.quality ?? "-"}, weather=${m.weather ?? "-"}, day_time=${m.day_time ?? "-"}, scene=${m.scene_layout ?? "-"}`;
  }

  function resizeCanvas() {
    const rect = videoWrap.getBoundingClientRect();
    const nextWidth = Math.max(1, Math.round(rect.width || video.clientWidth || 1));
    const nextHeight = Math.max(1, Math.round(rect.height || video.clientHeight || 1));
    const sizeChanged = canvas.width !== nextWidth || canvas.height !== nextHeight;
    if (sizeChanged) {
      canvas.width = nextWidth;
      canvas.height = nextHeight;
      applyZoomTransform();
    }
  }

  function showVideoStatus(text) {
    if (videoStatusEl) {
      videoStatusEl.textContent = text || "";
      videoStatusEl.style.color = text ? "#9f1239" : "#5a6670";
    }
  }

  function showVideoSuccess(text) {
    if (videoStatusEl) {
      videoStatusEl.textContent = text || "";
      videoStatusEl.style.color = text ? "#15803d" : "#5a6670";
    }
  }

  function seekVideoToDraftTime() {
    if (!isEditEnabled()) {
      return;
    }
    const target = num(tEl.value);
    if (target == null || target < 0) {
      return;
    }
    const maxT = Number.isFinite(video.duration) ? video.duration : null;
    const clamped = maxT == null ? target : Math.min(target, Math.max(0, maxT));
    video.currentTime = clamped;
  }

  async function refreshHistory() {
    const res = await fetch(`/api/gt/history?source=${encodeURIComponent(payload.source || "test")}&video_path=${encodeURIComponent(payload.video_path)}`);
    const data = await res.json();
    const items = data.items || [];
    historyBody.innerHTML = "";
    items.forEach((h) => {
      const tr = document.createElement("tr");

      const tdTime = document.createElement("td");
      tdTime.style.verticalAlign = "top";
      tdTime.style.borderTop = "1px solid #ddd";
      tdTime.style.padding = "6px";
      tdTime.textContent = h.edited_at || "";

      const tdEditor = document.createElement("td");
      tdEditor.style.verticalAlign = "top";
      tdEditor.style.borderTop = "1px solid #ddd";
      tdEditor.style.padding = "6px";
      tdEditor.textContent = h.edited_by || "";

      const tdAfter = document.createElement("td");
      tdAfter.style.verticalAlign = "top";
      tdAfter.style.borderTop = "1px solid #ddd";
      tdAfter.style.padding = "6px";
      tdAfter.style.whiteSpace = "pre-wrap";
      tdAfter.textContent = h.after_json || "";

      tr.appendChild(tdTime);
      tr.appendChild(tdEditor);
      tr.appendChild(tdAfter);
      historyBody.appendChild(tr);
    });
  }

  function draw() {
    if (!video.videoWidth || !video.videoHeight) {
      requestAnimationFrame(draw);
      return;
    }
    resizeCanvas();
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    drawMarker(pred.center_x, pred.center_y, "#ef4444", "Pred");
    if (isEditEnabled() && hoverPoint && !isDraggingGt) {
      drawMarker(hoverPoint.x, hoverPoint.y, "#22c55e", "GT");
    } else {
      const renderGt = getRenderGt();
      drawMarker(renderGt.center_x, renderGt.center_y, "#22c55e", "GT");
    }
    drawZoomGuide();
    drawEditGuide();
    drawInfo();
    requestAnimationFrame(draw);
  }

  function updateDraftPointFromEvent(e) {
    const p = pointFromEvent(e);
    const x = p.x;
    const y = p.y;
    cxEl.value = x.toFixed(4);
    cyEl.value = y.toFixed(4);
    syncDraftFromForm();
    renderHUDText();
    cursorPos.textContent = `편집 좌표: x=${x.toFixed(4)}, y=${y.toFixed(4)}`;
    if (saveMsg) {
      saveMsg.textContent = "좌표를 이동했습니다. 저장 버튼을 누르면 반영됩니다.";
    }
    if (saveState) {
      saveState.textContent = "미저장 변경 있음";
    }
  }

  canvas.addEventListener("click", (e) => {
    if (isZoomSelectEnabled()) {
      return;
    }
    if (!isEditEnabled()) {
      return;
    }
    if (isDraggingGt) {
      return;
    }
    updateDraftPointFromEvent(e);
  });

  canvas.addEventListener("mousedown", (e) => {
    if (isZoomSelectEnabled()) {
      isDraggingZoom = true;
      zoomSelectionStart = pointFromEvent(e);
      zoomPreviewRect = { x: zoomSelectionStart.x, y: zoomSelectionStart.y, w: 0, h: 0 };
      syncZoomSelectionBox();
      if (cursorPos) {
        cursorPos.textContent = `확대 시작 좌표: x=${zoomSelectionStart.x.toFixed(4)}, y=${zoomSelectionStart.y.toFixed(4)}`;
      }
      return;
    }
    if (!isEditEnabled()) {
      return;
    }
    isDraggingGt = true;
    hoverPoint = null;
    updateDraftPointFromEvent(e);
  });

  canvas.addEventListener("mousemove", (e) => {
    if (isZoomSelectEnabled()) {
      const p = pointFromEvent(e);
      if (isDraggingZoom && zoomSelectionStart) {
        zoomPreviewRect = rectFromPoints(zoomSelectionStart, p);
        syncZoomSelectionBox();
        if (cursorPos) {
          cursorPos.textContent = `확대 후보: x=${zoomPreviewRect.x.toFixed(4)}, y=${zoomPreviewRect.y.toFixed(4)}, w=${zoomPreviewRect.w.toFixed(4)}, h=${zoomPreviewRect.h.toFixed(4)}`;
        }
      } else if (cursorPos) {
        cursorPos.textContent = `확대 미리보기 좌표: x=${p.x.toFixed(4)}, y=${p.y.toFixed(4)}`;
      }
      return;
    }
    if (!isEditEnabled()) {
      return;
    }
    if (isDraggingGt) {
      updateDraftPointFromEvent(e);
      return;
    }
    hoverPoint = pointFromEvent(e);
    if (cursorPos) {
      cursorPos.textContent = `미리보기 좌표: x=${hoverPoint.x.toFixed(4)}, y=${hoverPoint.y.toFixed(4)} (드래그하면 반영)`;
    }
  });

  window.addEventListener("mouseup", () => {
    if (isDraggingZoom) {
      isDraggingZoom = false;
      if (zoomPreviewRect && zoomPreviewRect.w >= MIN_ZOOM_RECT_SIZE && zoomPreviewRect.h >= MIN_ZOOM_RECT_SIZE) {
        setZoomRect(zoomPreviewRect);
        if (zoomModeEl) {
          zoomModeEl.checked = false;
        }
        showVideoSuccess("선택 영역 확대 적용");
      } else if (zoomPreviewRect) {
        if (zoomStateEl) {
          zoomStateEl.textContent = "선택 영역이 너무 작습니다. 더 넓게 드래그해 주세요.";
        }
      }
      clearZoomPreview();
      applyEditModeUI();
      return;
    }
    if (isDraggingGt && saveMsg) {
      saveMsg.textContent = "드래그 완료. 저장 버튼을 누르면 서버에 반영됩니다.";
    }
    isDraggingGt = false;
  });

  canvas.addEventListener("mouseleave", () => {
    if (!isDraggingZoom) {
      hoverPoint = null;
    }
    isDraggingGt = false;
  });

  [tEl, cxEl, cyEl, typeEl, noteEl].forEach((el) => {
    el.addEventListener("input", () => {
      syncDraftFromForm();
      if (el === tEl) {
        seekVideoToDraftTime();
      }
      renderHUDText();
      if (saveState) {
        saveState.textContent = "미저장 변경 있음";
      }
    });
  });

  editModeEl?.addEventListener("change", () => {
    if (editModeEl.checked && zoomModeEl) {
      zoomModeEl.checked = false;
      clearZoomPreview();
    }
    applyEditModeUI();
    renderHUDText();
  });

  toggleEditModeBtn?.addEventListener("click", () => {
    if (!editModeEl) {
      return;
    }
    if (!editModeEl.checked && zoomModeEl) {
      zoomModeEl.checked = false;
      clearZoomPreview();
    }
    editModeEl.checked = !editModeEl.checked;
    applyEditModeUI();
    renderHUDText();
  });

  toggleZoomModeBtn?.addEventListener("click", () => {
    if (!zoomModeEl) {
      return;
    }
    zoomModeEl.checked = !zoomModeEl.checked;
    if (zoomModeEl.checked && editModeEl) {
      editModeEl.checked = false;
    }
    clearZoomPreview();
    applyEditModeUI();
  });

  resetZoomBtn?.addEventListener("click", () => {
    resetZoom();
  });

  playbackRateButtons.forEach((button) => {
    button.addEventListener("click", () => {
      applyPlaybackRate(button.dataset.playbackRate || "1");
    });
  });

  setTimeFromVideoBtn?.addEventListener("click", () => {
    tEl.value = (video.currentTime || 0).toFixed(2);
    syncDraftFromForm();
    renderHUDText();
    if (saveMsg) {
      saveMsg.textContent = `현재 재생시간 ${tEl.value}s 를 accident_time에 적용했습니다. 저장 버튼을 누르면 확정됩니다.`;
    }
    if (saveState) {
      saveState.textContent = "미저장 변경 있음";
    }
  });

  video.addEventListener("timeupdate", () => {
    if (playbackTimeDisplayEl) {
      playbackTimeDisplayEl.textContent = `현재 재생시간: ${(video.currentTime || 0).toFixed(2)}s`;
    }
  });

  video.addEventListener("ratechange", () => {
    const rate = clamp(num(video.playbackRate, 1), 0.25, 2);
    syncPlaybackRateUI(rate);
    localStorage.setItem(PLAYBACK_RATE_STORAGE_KEY, String(rate));
  });

  video.addEventListener("error", () => {
    const err = video.error;
    const code = err ? err.code : "unknown";
    showVideoStatus(`비디오 로드 실패(code=${code}). 직접 열기로 확인해 주세요.`);
  });

  video.addEventListener("loadeddata", () => {
    applyPlaybackRate(localStorage.getItem(PLAYBACK_RATE_STORAGE_KEY) || "1", { persist: false });
    showVideoSuccess("비디오 로드 완료");
    buildEventMarkers();
    if (loadTimeoutId) {
      clearTimeout(loadTimeoutId);
      loadTimeoutId = null;
    }
  });

  video.addEventListener("stalled", () => {
    showVideoStatus("네트워크 지연으로 재생이 멈췄습니다. 잠시 후 자동 복구되거나 직접 열기로 확인해 주세요.");
  });

  video.addEventListener("waiting", () => {
    showVideoStatus("버퍼링 중입니다...");
  });

  video.addEventListener("playing", () => {
    showVideoSuccess("재생 중");
  });

  document.getElementById("seek-pred").addEventListener("click", () => {
    if (pred.accident_time != null) {
      video.currentTime = Math.max(0, Number(pred.accident_time));
    }
  });

  document.getElementById("seek-gt").addEventListener("click", () => {
    const renderGt = getRenderGt();
    if (renderGt.accident_time != null) {
      video.currentTime = Math.max(0, Number(renderGt.accident_time));
    }
  });

  document.getElementById("copy-pred").addEventListener("click", () => {
    tEl.value = pred.accident_time ?? "";
    cxEl.value = pred.center_x ?? "";
    cyEl.value = pred.center_y ?? "";
    typeEl.value = pred.type ?? "";
    syncDraftFromForm();
    renderHUDText();
  });

  document.getElementById("save-gt").addEventListener("click", async () => {
    const editor = (editorEl.value || "").trim() || localStorage.getItem("gt_editor") || "anonymous";
    localStorage.setItem("gt_editor", editor);

    const body = {
      source: payload.source || "test",
      video_path: payload.video_path,
      editor,
      accident_time: draftGt.accident_time,
      center_x: draftGt.center_x,
      center_y: draftGt.center_y,
      type: draftGt.type || "",
      note: draftGt.note || "",
      base_updated_at: gt.updated_at || null,
    };

    saveMsg.textContent = "저장 중...";

    const res = await fetch("/api/gt", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const data = await res.json();
    if (!res.ok) {
      if (res.status === 409 && data.detail && data.detail.current) {
        const current = data.detail.current;
        gt = current;
        syncFormFromGt();
        renderHUDText();
        saveMsg.textContent = "저장 충돌: 최신 값으로 갱신했습니다. 확인 후 다시 저장하세요.";
        if (saveState) {
          saveState.textContent = "다른 편집자가 먼저 저장했습니다. 최신 값으로 동기화됨.";
        }
        return;
      }
      saveMsg.textContent = `저장 실패: ${data.detail || "unknown"}`;
      if (saveState) {
        saveState.textContent = "저장 실패";
      }
      return;
    }

    gt = data.record;
    draftGt = { ...gt };
    hoverPoint = null;
    isDraggingGt = false;
    if (editModeEl) {
      editModeEl.checked = false;
    }
    applyEditModeUI();
    syncFormFromGt();
    renderHUDText();
    buildEventMarkers();
    refreshHistory();
    saveMsg.textContent = "저장 완료: GT 오버레이와 우측 값이 즉시 반영되었습니다.";
    if (saveState) {
      saveState.textContent = `마지막 저장: ${gt.updated_at} / 편집자: ${gt.updated_by} / 새로고침 불필요`;
    }
    showVideoSuccess("저장 완료: 최신 GT 반영됨");
  });

  if (openVideoDirectEl) {
    const directUrl = `/media?source=${encodeURIComponent(payload.source || "test")}&video_path=${encodeURIComponent(payload.video_path)}`;
    openVideoDirectEl.setAttribute("href", directUrl);
  }

  editorEl.value = localStorage.getItem("gt_editor") || "";
  if (!editorEl.value && payload.user) {
    editorEl.value = payload.user;
  }
  syncFormFromGt();
  syncDraftFromForm();
  loadSavedPlaybackRate();
  if (editModeEl) {
    editModeEl.checked = false;
  }
  applyEditModeUI();
  renderHUDText();
  video.addEventListener("loadedmetadata", resizeCanvas);
  video.addEventListener("loadedmetadata", buildEventMarkers);
  window.addEventListener("resize", resizeCanvas);
  if (video.currentSrc || video.src) {
    video.setAttribute("data-base-src", video.currentSrc || video.src);
  }
  loadTimeoutId = window.setTimeout(() => {
    if (video.readyState < 2) {
      showVideoStatus("비디오 로드가 지연되고 있습니다. 잠시 기다리거나 직접 열기를 사용해 주세요.");
    }
  }, 8000);
  requestAnimationFrame(draw);
})();
