document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('qform');
  if (!form) return;

  // Clear all filters & sorts (keeps limit)
  const clearBtn = document.getElementById('clearBtn');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      const limitInput = form.querySelector('input[name="limit"]');
      const limit = limitInput ? limitInput.value : '100';

      window.location.href =
        window.location.pathname +
        '?limit=' + encodeURIComponent(limit) +
        '&format=html';
    });
  }

  // Clear sorts only (keeps filters + limit)
  const clearSortsBtn = document.getElementById('clearSortsBtn');
  if (clearSortsBtn) {
    clearSortsBtn.addEventListener('click', () => {
      form.querySelectorAll('input[type="hidden"][name^="s_"]').forEach(h => {
        h.value = '';
      });
      form.submit();
    });
  }

  // Header click sorting: none → asc → desc → none
  document.querySelectorAll('th[data-col] [data-action="sort"]').forEach(el => {
    el.addEventListener('click', e => {
      const th = e.currentTarget.closest('th[data-col]');
      const col = th.getAttribute('data-col');
      const current = th.getAttribute('data-sort') || '';

      const next =
        current === '' ? 'asc' :
        current === 'asc' ? 'desc' :
        '';

      const hid = th.querySelector(
        'input[type="hidden"][name="s_' + col + '"]'
      );
      if (hid) hid.value = next;

      form.submit();
    });
  });

  // Chip remove handlers
  document.querySelectorAll('.chip .x').forEach(x => {
    x.addEventListener('click', e => {
      const chip = e.currentTarget.closest('.chip');
      const col = chip.getAttribute('data-col');
      const kind = chip.getAttribute('data-kind');
      if (!col || !kind) return;

      if (kind === 'filter') {
        const inp = form.querySelector('input[name="f_' + col + '"]');
        if (inp) inp.value = '';
      } else if (kind === 'sort') {
        const hid = form.querySelector(
          'input[type="hidden"][name="s_' + col + '"]'
        );
        if (hid) hid.value = '';
      }

      form.submit();
    });
  });
});
