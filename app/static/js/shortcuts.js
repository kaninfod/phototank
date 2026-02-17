(function () {
  function isTypingInInput() {
    const el = document.activeElement;
    if (!el) return false;
    const tag = (el.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea' || tag === 'select') return true;
    if (el.isContentEditable) return true;
    return false;
  }

  function scrollIntoViewNearest(el) {
    try {
      el.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    } catch {
      // ignore
    }
  }

  function parseDigitKey(e) {
    if (e.key === '0' || e.key === '1' || e.key === '2' || e.key === '3') {
      return Number.parseInt(e.key, 10);
    }
    return null;
  }

  // ---------- Grid navigation ----------

  function gridTiles() {
    return Array.from(document.querySelectorAll('.photo-tile[data-guid]'));
  }

  function tileCenter(tile) {
    const r = tile.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
  }

  function setActiveTile(tile) {
    const prev = document.querySelector('.photo-tile.kb-active');
    if (prev && prev !== tile) prev.classList.remove('kb-active');
    tile.classList.add('kb-active');
    scrollIntoViewNearest(tile);
  }

  function getActiveTile(tiles) {
    const active = document.querySelector('.photo-tile.kb-active');
    if (active && tiles.includes(active)) return active;
    return null;
  }

  function moveSpatial(tiles, current, dir) {
    const c = tileCenter(current);

    let best = null;
    let bestScore = Infinity;

    for (const t of tiles) {
      if (t === current) continue;
      const p = tileCenter(t);
      const dx = p.x - c.x;
      const dy = p.y - c.y;

      if (dir === 'left' && dx >= -1) continue;
      if (dir === 'right' && dx <= 1) continue;
      if (dir === 'up' && dy >= -1) continue;
      if (dir === 'down' && dy <= 1) continue;

      // Prefer roughly same row for left/right, same column for up/down.
      const w = (dir === 'left' || dir === 'right') ? 2.5 : 2.0;
      const score = (dx * dx) + (dy * w) * (dy * w);

      if (score < bestScore) {
        bestScore = score;
        best = t;
      }
    }

    return best;
  }

  function openActiveTile(tile) {
    const link = tile.querySelector('a.stretched-link');
    const href = link ? link.getAttribute('href') : null;
    if (href) window.location.assign(href);
  }

  function toggleSelectGuidInGrid(guid) {
    const cb = document.querySelector(`.select-photo[data-guid="${CSS.escape(guid)}"]`);
    if (!cb) return;
    cb.checked = !cb.checked;
    cb.dispatchEvent(new Event('change', { bubbles: true }));
  }

  function setRatingForGuid(guid, rating) {
    if (window.phototankRating && typeof window.phototankRating.set === 'function') {
      window.phototankRating.set(guid, rating);
      return;
    }

    // Fallback: if rating helper isn’t present, do nothing.
  }

  function handleGridKeydown(e) {
    const tiles = gridTiles();
    if (tiles.length === 0) return false;

    if (isTypingInInput()) return false;

    // Page navigation shortcuts (grid view): n=newer, o/b=older
    if (!e.metaKey && !e.ctrlKey && !e.altKey) {
      const k = (e.key || '').toLowerCase();
      if (k === 'n' || k === 'o' || k === 'b') {
        // In the newest-first gallery, the left button is "Newer" (id=btnPrevPage)
        // and the right button is "Older" (id=btnNextPage).
        const sel = k === 'n' ? '#btnPrevPage' : '#btnNextPage';
        const link = document.querySelector(sel);
        const href = link ? link.getAttribute('href') : null;
        if (href) {
          e.preventDefault();
          link.click();
          return true;
        }
      }
    }

    const key = e.key;

    const digit = parseDigitKey(e);
    if (digit !== null) {
      const active = getActiveTile(tiles);
      if (!active) return false;
      const guid = active.dataset.guid;
      if (!guid) return false;
      e.preventDefault();
      setRatingForGuid(guid, digit);
      return true;
    }

    if (key === ' ') {
      const active = getActiveTile(tiles);
      if (!active) return false;
      const guid = active.dataset.guid;
      if (!guid) return false;
      e.preventDefault();
      toggleSelectGuidInGrid(guid);
      return true;
    }

    if (key === 'Enter') {
      const active = getActiveTile(tiles);
      if (!active) return false;
      e.preventDefault();
      openActiveTile(active);
      return true;
    }

    let dir = null;
    if (key === 'ArrowLeft') dir = 'left';
    else if (key === 'ArrowRight') dir = 'right';
    else if (key === 'ArrowUp') dir = 'up';
    else if (key === 'ArrowDown') dir = 'down';

    if (!dir) return false;

    e.preventDefault();

    const active = getActiveTile(tiles);
    if (!active) {
      setActiveTile(tiles[0]);
      return true;
    }

    const next = moveSpatial(tiles, active, dir);
    if (next) setActiveTile(next);
    return true;
  }

  // ---------- Detail shortcuts ----------

  function currentDetailGuid() {
    const badge = document.querySelector('#navMain .rating-badge[data-guid], #photoPanel .rating-badge[data-guid]');
    if (badge && badge.dataset.guid) return badge.dataset.guid;
    const cb = document.querySelector('#navMain .select-detail[data-guid], #photoPanel .select-detail[data-guid]');
    if (cb && cb.dataset.guid) return cb.dataset.guid;
    return null;
  }

  function toggleSelectInDetail(guid) {
    const cb = document.querySelector(`#navMain .select-detail[data-guid="${CSS.escape(guid)}"], #photoPanel .select-detail[data-guid="${CSS.escape(guid)}"]`);
    if (!cb) return;
    cb.checked = !cb.checked;
    cb.dispatchEvent(new Event('change', { bubbles: true }));
  }

  function clickIfEnabled(sel) {
    const el = document.querySelector(sel);
    if (!el) return false;
    if (el.tagName.toLowerCase() === 'button' && el.disabled) return false;
    el.click();
    return true;
  }

  function handleDetailKeydown(e) {
    const panel = document.getElementById('photoPanel');
    if (!panel) return false;

    if (isTypingInInput()) return false;

    // Don't steal Cmd+Arrow; reserve it for grid pagination / browser navigation.
    if (e.metaKey) return false;

    const key = e.key;

    const digit = parseDigitKey(e);
    if (digit !== null) {
      const guid = currentDetailGuid();
      if (!guid) return false;
      e.preventDefault();
      setRatingForGuid(guid, digit);
      return true;
    }

    if (key === ' ') {
      const guid = currentDetailGuid();
      if (!guid) return false;
      e.preventDefault();
      toggleSelectInDetail(guid);
      return true;
    }

    if (key === 'Escape') {
      e.preventDefault();
      // Photo Grid button
      clickIfEnabled('#btnGrid');
      return true;
    }

    if (key === 'ArrowLeft') {
      e.preventDefault();
      clickIfEnabled('#btnPrev');
      return true;
    }

    if (key === 'ArrowRight') {
      e.preventDefault();
      clickIfEnabled('#btnNext');
      return true;
    }

    return false;
  }

  function onKeydown(e) {
    // Prefer detail handling when detail panel is present.
    if (handleDetailKeydown(e)) return;
    handleGridKeydown(e);
  }

  function init() {
    // Ensure only one listener.
    if (window.__phototankShortcutsBound) return;
    window.__phototankShortcutsBound = true;

    window.addEventListener('keydown', onKeydown);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
