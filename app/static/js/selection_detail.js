(function () {
  const KEY = 'phototank_selected';

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

  function initOne(cb) {
    if (!cb || cb.dataset.bound === '1') return;
    cb.dataset.bound = '1';

    const guid = cb.dataset.guid;
    if (!guid) return;

    const selected = loadSet();
    cb.checked = selected.has(guid);

    cb.addEventListener('click', (e) => {
      // Don’t let any parent click handlers navigate.
      e.stopPropagation();
    });

    cb.addEventListener('change', () => {
      const s = loadSet();
      if (cb.checked) s.add(guid);
      else s.delete(guid);
      saveSet(s);
    });
  }

  function initAll() {
    const boxes = Array.from(document.querySelectorAll('.select-detail[data-guid]'));
    for (const cb of boxes) initOne(cb);
  }

  document.addEventListener('DOMContentLoaded', initAll);
  document.addEventListener('htmx:load', initAll);
})();
