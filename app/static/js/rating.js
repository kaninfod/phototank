(function () {
  function normalizeRating(v) {
    const n = Number.parseInt(String(v), 10);
    if (Number.isNaN(n)) return 0;
    return Math.min(3, Math.max(0, n));
  }

  function nextRating(current) {
    return (current + 1) % 4;
  }

  function badgeClassesFor(rating) {
    // Bootstrap-only: grey for 0, "gold-ish" via warning for 1..3.
    return rating === 0
      ? ['bg-secondary', 'text-light']
      : ['bg-warning', 'text-dark'];
  }

  function applyBadge(badge, rating) {
    badge.textContent = String(rating);
    badge.dataset.rating = String(rating);

    badge.classList.remove('bg-secondary', 'text-light', 'bg-warning', 'text-dark');
    const cls = badgeClassesFor(rating);
    for (const c of cls) badge.classList.add(c);

    const guid = badge.dataset.guid || '';
    badge.setAttribute('aria-label', guid ? `Rating ${rating} for ${guid}` : `Rating ${rating}`);
  }

  async function setRating(rateUrl, guid, rating) {
    const resp = await fetch(rateUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ guid, rating }),
    });

    let payload = null;
    try { payload = await resp.json(); } catch { /* ignore */ }

    if (!resp.ok) {
      const msg = payload && payload.detail ? payload.detail : 'Rating update failed';
      throw new Error(msg);
    }

    return payload;
  }

  // Expose a tiny API for other scripts (e.g. keyboard shortcuts).
  function ensureGlobalApi() {
    if (!window.phototankRating) {
      window.phototankRating = {};
    }

    if (typeof window.phototankRating.set !== 'function') {
      window.phototankRating.set = async function (guid, rating) {
        const badges = Array.from(document.querySelectorAll(`.rating-badge[data-guid="${CSS.escape(guid)}"]`));
        if (badges.length === 0) return;

        const rateUrl = (document.body && document.body.dataset && document.body.dataset.rateUrl)
          ? document.body.dataset.rateUrl
          : '/phototank/rate';

        const desired = normalizeRating(rating);
        const current = normalizeRating(badges[0].dataset.rating ?? badges[0].textContent);

        for (const bb of badges) applyBadge(bb, desired);
        try {
          await setRating(rateUrl, guid, desired);
        } catch (err) {
          for (const bb of badges) applyBadge(bb, current);
          alert(err && err.message ? err.message : 'Rating update failed');
        }
      };
    }
  }

  function init() {
    ensureGlobalApi();
    const badges = Array.from(document.querySelectorAll('.rating-badge[data-guid]'));
    if (badges.length === 0) return;

    const rateUrl = (document.body && document.body.dataset && document.body.dataset.rateUrl)
      ? document.body.dataset.rateUrl
      : '/phototank/rate';

    // Initial paint from dataset/text.
    for (const b of badges) {
      const r = normalizeRating(b.dataset.rating ?? b.textContent);
      applyBadge(b, r);
    }

    for (const b of badges) {
      if (b.dataset.ratingBound === '1') continue;
      b.dataset.ratingBound = '1';

      b.addEventListener('click', async (e) => {
        // Avoid triggering navigation via stretched-link overlay.
        e.preventDefault();
        e.stopPropagation();

        const guid = b.dataset.guid;
        if (!guid) return;

        const current = normalizeRating(b.dataset.rating ?? b.textContent);
        const desired = nextRating(current);

        // Optimistic UI update across all badges for this guid.
        const all = Array.from(document.querySelectorAll(`.rating-badge[data-guid="${CSS.escape(guid)}"]`));
        for (const bb of all) applyBadge(bb, desired);

        try {
          await setRating(rateUrl, guid, desired);
        } catch (err) {
          // Revert on failure.
          for (const bb of all) applyBadge(bb, current);
          alert(err && err.message ? err.message : 'Rating update failed');
        }
      });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Re-run after HTMX swaps in new content.
  document.addEventListener('htmx:load', init);
})();
