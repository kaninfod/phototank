(function () {
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

  function loadSelectedSet() {
    try {
      const raw = localStorage.getItem('phototank_selected');
      if (!raw) return [];
      const arr = JSON.parse(raw);
      if (!Array.isArray(arr)) return [];
      return arr;
    } catch {
      return [];
    }
  }

  async function postJson(url, payload) {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    let data = null;
    try { data = await resp.json(); } catch { /* ignore */ }

    if (!resp.ok) {
      const msg = data ? formatDetail(data.detail) : null;
      throw new Error(msg || `Request failed (${resp.status})`);
    }

    return data;
  }

  function initGalleryOffcanvasTagging() {
    const tagSelect = document.getElementById('offcanvasTagSelect');
    const applyBtn = document.getElementById('offcanvasTagApply');
    const removeBtn = document.getElementById('offcanvasTagRemove');

    const newName = document.getElementById('newTagName');
    const newDesc = document.getElementById('newTagDesc');
    const newColor = document.getElementById('newTagColor');
    const createApplyBtn = document.getElementById('createAndApplyTag');

    if (!tagSelect || !applyBtn || !removeBtn) return;

    function selectedGuidsOrAlert() {
      const guids = loadSelectedSet();
      if (guids.length === 0) {
        alert('No photos selected.');
        return null;
      }
      return guids;
    }

    applyBtn.addEventListener('click', async () => {
      const guids = selectedGuidsOrAlert();
      if (!guids) return;
      const tagId = tagSelect.value;
      if (!tagId) { alert('Select a tag first.'); return; }

      try {
        await postJson(`/phototank/tags/${encodeURIComponent(tagId)}/apply`, { guids });
        alert('Tag applied.');
      } catch (e) {
        alert(e && e.message ? e.message : String(e));
      }
    });

    removeBtn.addEventListener('click', async () => {
      const guids = selectedGuidsOrAlert();
      if (!guids) return;
      const tagId = tagSelect.value;
      if (!tagId) { alert('Select a tag first.'); return; }

      try {
        await postJson(`/phototank/tags/${encodeURIComponent(tagId)}/remove`, { guids });
        alert('Tag removed.');
      } catch (e) {
        alert(e && e.message ? e.message : String(e));
      }
    });

    if (createApplyBtn && newName && newColor) {
      createApplyBtn.addEventListener('click', async () => {
        const guids = selectedGuidsOrAlert();
        if (!guids) return;

        const name = (newName.value || '').trim();
        if (!name) { alert('Enter a tag name.'); return; }

        const payload = {
          name,
          description: newDesc ? (newDesc.value || '').trim() : null,
          color: (newColor.value || 'primary'),
        };

        try {
          const created = await postJson('/phototank/tags', payload);

          // Ensure tag exists in dropdown and select it.
          if (created && created.id && created.name) {
            const idStr = String(created.id);
            let opt = Array.from(tagSelect.options).find((o) => o.value === idStr);
            if (!opt) {
              opt = document.createElement('option');
              opt.value = idStr;
              opt.textContent = created.name;
              tagSelect.appendChild(opt);
            }
            tagSelect.value = idStr;
          }

          await postJson(`/phototank/tags/${encodeURIComponent(tagSelect.value)}/apply`, { guids });
          alert('Tag created and applied.');

          // Clear inputs.
          newName.value = '';
          if (newDesc) newDesc.value = '';
        } catch (e) {
          alert(e && e.message ? e.message : String(e));
        }
      });
    }
  }

  function initDetailTagging() {
    const tagsWrap = document.getElementById('photoTags');
    if (!tagsWrap) return;

    const guid = tagsWrap.dataset.photoGuid;
    if (!guid) return;

    const addSelect = document.getElementById('detailTagSelect');
    const addBtn = document.getElementById('detailTagAdd');

    if (addBtn && addSelect) {
      addBtn.addEventListener('click', async () => {
        const tagId = addSelect.value;
        if (!tagId) { alert('Select a tag first.'); return; }
        try {
          await postJson(`/phototank/tags/${encodeURIComponent(tagId)}/apply`, { guids: [guid] });
          location.reload();
        } catch (e) {
          alert(e && e.message ? e.message : String(e));
        }
      });
    }

    const removeButtons = Array.from(tagsWrap.querySelectorAll('button.tag-remove[data-tag-id]'));
    for (const btn of removeButtons) {
      btn.addEventListener('click', async () => {
        const tagId = btn.dataset.tagId;
        if (!tagId) return;
        try {
          await postJson(`/phototank/tags/${encodeURIComponent(tagId)}/remove`, { guids: [guid] });
          location.reload();
        } catch (e) {
          alert(e && e.message ? e.message : String(e));
        }
      });
    }
  }

  function init() {
    initGalleryOffcanvasTagging();
    initDetailTagging();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
