(function () {
  const KEY = 'phototank_selected';

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

    const out = new URLSearchParams();
    if (jump) out.set('jump', jump);
    if (rating !== null && rating !== '') out.set('rating', rating);
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
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
