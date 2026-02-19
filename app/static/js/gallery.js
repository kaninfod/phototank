(function () {
  const KEY = 'phototank_selected';

  function initMobileMasonry() {
    const mq = window.matchMedia && window.matchMedia('(max-width: 575.98px)');
    if (!mq || !mq.matches) return;

    const grid = document.querySelector('.phototank-gallery-grid');
    if (!grid) return;

    let layoutTimer = null;

    function scheduleLayout() {
      if (layoutTimer) return;
      layoutTimer = window.setTimeout(() => {
        layoutTimer = null;
        layout();
      }, 50);
    }

    function layout() {
      // Turn on masonry mode (tiny fixed rows) only while we also apply spans,
      // otherwise tiles default to 1-row and their contents overlap.
      grid.classList.add('phototank-masonry');

      const styles = window.getComputedStyle(grid);
      const rowHeight = parseFloat(styles.getPropertyValue('grid-auto-rows')) || 8;
      const rowGap = parseFloat(styles.getPropertyValue('row-gap')) || 0;

      const tiles = Array.from(grid.querySelectorAll(':scope > .col'));
      for (const tile of tiles) {
        tile.classList.remove('phototank-wide');

        const img = tile.querySelector('img.thumb-img');
        if (img && img.naturalWidth && img.naturalHeight) {
          const ratio = img.naturalWidth / img.naturalHeight;
          if (ratio >= 1.25) tile.classList.add('phototank-wide');
        }
      }

      for (const tile of tiles) {
        const card = tile.querySelector('.photo-tile') || tile;

        // Measure the content height (the inner card), not the grid item box.
        // If we measure the grid item while it has a too-small span, we can
        // under-measure and cause overlaps.
        let height = card.getBoundingClientRect().height;

        // If the image isn't loaded yet, height may be ~0; reserve a reasonable placeholder.
        if (!height || height < 16) {
          const w = tile.getBoundingClientRect().width || 160;
          height = w * 1.35;
        }

        const span = Math.max(1, Math.ceil((height + rowGap) / (rowHeight + rowGap)));
        tile.style.gridRowEnd = `span ${span}`;
      }
    }

    // Initial layout once images are ready; also relayout on each image load.
    const imgs = Array.from(grid.querySelectorAll('img.thumb-img'));
    for (const img of imgs) {
      if (img.complete) continue;
      img.addEventListener('load', scheduleLayout, { once: true });
      img.addEventListener('error', scheduleLayout, { once: true });
    }

    window.addEventListener('resize', scheduleLayout);
    // Run after first paint so the base (non-masonry) grid can size normally,
    // then switch into masonry mode with spans.
    window.requestAnimationFrame(() => scheduleLayout());
  }

  function formatDetail(detail) {
    if (!detail) return null;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
      const msgs = detail
        .map((e) => (e && typeof e.msg === 'string') ? e.msg : null)
        .filter(Boolean);
      if (msgs.length > 0) return msgs.join('\n');
      try { return JSON.stringify(detail); } catch { return String(detail); }
    }
    try { return JSON.stringify(detail); } catch { return String(detail); }
  }

  function loadSet() {
    try {
      const raw = localStorage.getItem(KEY);
      if (!raw) return new Set();
      const arr = JSON.parse(raw);
      if (!Array.isArray(arr)) return new Set();
      return new Set(arr);
    } catch {
      return new Set();
    }
  }

  function saveSet(set) {
    localStorage.setItem(KEY, JSON.stringify(Array.from(set)));
  }

  function currentFromParam() {
    return encodeURIComponent(location.pathname + location.search);
  }

  function currentDetailContextQuery() {
    // Carry the current gallery filter context into detail URLs.
    const params = new URLSearchParams(location.search || '');
    const jump = params.get('jump') || params.get('start');
    const rating = params.get('rating');
    const tag = params.get('tag');

    const out = new URLSearchParams();
    if (jump) out.set('jump', jump);
    if (rating !== null && rating !== '') out.set('rating', rating);
    if (tag !== null && tag !== '') out.set('tag', tag);
    return out.toString();
  }

  function init() {
    const checkboxes = Array.from(document.querySelectorAll('.select-photo'));
    const navCount = document.getElementById('navSelectedCount');
    const offCount = document.getElementById('offcanvasCount');
    const grid = document.getElementById('selectedGrid');
    const clearBtn = document.getElementById('offcanvasClear');
    const deleteBtn = document.getElementById('offcanvasDelete');

    const deleteUrl = (document.body && document.body.dataset && document.body.dataset.deleteUrl)
      ? document.body.dataset.deleteUrl
      : '/phototank/delete';

    function updateUI(set) {
      if (navCount) navCount.textContent = String(set.size);
      if (offCount) offCount.textContent = String(set.size);
      for (const cb of checkboxes) {
        const guid = cb.dataset.guid;
        cb.checked = guid && set.has(guid);
      }
    }

    function renderGrid(set) {
      if (!grid) return;
      grid.innerHTML = '';
      const guids = Array.from(set);

      const ctx = currentDetailContextQuery();

      for (const guid of guids) {
        const wrap = document.createElement('div');
        wrap.className = 'position-relative';

        const link = document.createElement('a');
        link.href = `/phototank/photo/${guid}?${ctx ? (ctx + '&') : ''}from=${currentFromParam()}`;
        link.className = 'd-block';

        const img = document.createElement('img');
        img.src = `/phototank/thumb/${guid}`;
        img.loading = 'lazy';
        img.className = 'mini-thumb rounded border';
        img.alt = guid;

        link.appendChild(img);
        wrap.appendChild(link);

        const rm = document.createElement('button');
        rm.type = 'button';
        rm.className = 'btn btn-sm btn-dark position-absolute top-0 end-0 m-1';
        rm.textContent = '×';
        rm.setAttribute('aria-label', 'Remove from selection');
        rm.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          selected.delete(guid);
          saveSet(selected);
          updateUI(selected);
          renderGrid(selected);
        });

        wrap.appendChild(rm);
        grid.appendChild(wrap);
      }
    }

    let selected = loadSet();
    updateUI(selected);
    renderGrid(selected);

    for (const cb of checkboxes) {
      cb.addEventListener('click', (e) => {
        // Prevent the stretched-link from triggering navigation.
        e.stopPropagation();
      });

      cb.addEventListener('change', () => {
        const guid = cb.dataset.guid;
        if (!guid) return;
        if (cb.checked) selected.add(guid);
        else selected.delete(guid);
        saveSet(selected);
        updateUI(selected);
        renderGrid(selected);
      });
    }

    if (clearBtn) {
      clearBtn.addEventListener('click', () => {
        selected = new Set();
        saveSet(selected);
        updateUI(selected);
        renderGrid(selected);
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener('click', async () => {
        const guids = Array.from(selected);
        if (guids.length === 0) return;

        const ok = confirm(`Delete ${guids.length} photos? This cannot be undone.`);
        if (!ok) return;

        const resp = await fetch(deleteUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ guids }),
        });

        let payload = null;
        try { payload = await resp.json(); } catch { /* ignore */ }

        if (!resp.ok) {
          const msg = payload ? formatDetail(payload.detail) : null;
          alert(msg || `Delete failed (${resp.status})`);
          return;
        }

        if (payload && Array.isArray(payload.deleted_guids)) {
          for (const g of payload.deleted_guids) selected.delete(g);
          saveSet(selected);
        }

        location.reload();
      });
    }

    initMobileMasonry();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
