(function () {
  // ── Enter-badge visibility ───────────────────────────────────────────────
  var input = document.getElementById('q');
  var box   = document.getElementById('searchBox');
  if (input && box) {
    var sync = function () {
      if (input.value.trim().length > 0) box.classList.add('has-query');
      else box.classList.remove('has-query');
    };
    input.addEventListener('input', sync);
    sync();
  }

  // ── Filter chips ─────────────────────────────────────────────────────────
  var filters = document.querySelectorAll('.filter');
  if (filters.length) {
    filters.forEach(function (f) {
      f.addEventListener('click', function () {
        filters.forEach(function (x) {
          x.classList.remove('active');
          x.setAttribute('aria-selected', 'false');
        });
        f.classList.add('active');
        f.setAttribute('aria-selected', 'true');
      });
    });
  }

  // ── Autocomplete ─────────────────────────────────────────────────────────
  var suggestBox = document.getElementById('suggestBox');
  if (!input || !suggestBox) return;

  var debounceTimer = null;
  var activeIdx = -1;
  var currentSuggestions = [];

  function renderSuggestions(words, prefix) {
    suggestBox.innerHTML = '';
    activeIdx = -1;
    currentSuggestions = words;
    if (!words.length) { suggestBox.classList.remove('open'); return; }
    words.forEach(function (word, i) {
      var btn = document.createElement('button');
      btn.className = 'suggest-item';
      btn.type = 'button';
      // bold the prefix match
      var highlighted = '<em>' + escHtml(word.slice(0, prefix.length)) + '</em>' +
                        escHtml(word.slice(prefix.length));
      btn.innerHTML = highlighted;
      btn.addEventListener('mousedown', function (e) {
        e.preventDefault(); // keep focus on input
        input.value = word;
        sync && sync();
        closeSuggestions();
        input.form && input.form.submit();
      });
      suggestBox.appendChild(btn);
    });
    suggestBox.classList.add('open');
  }

  function closeSuggestions() {
    suggestBox.classList.remove('open');
    suggestBox.innerHTML = '';
    activeIdx = -1;
    currentSuggestions = [];
  }

  function escHtml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  input.addEventListener('input', function () {
    clearTimeout(debounceTimer);
    var val = input.value.trim();
    if (val.length < 2) { closeSuggestions(); return; }
    var prefix = val.toLowerCase().replace(/[^a-z0-9]/g, '');
    if (prefix.length < 2) { closeSuggestions(); return; }
    debounceTimer = setTimeout(function () {
      fetch('/api/suggest?q=' + encodeURIComponent(prefix))
        .then(function (r) { return r.json(); })
        .then(function (words) { renderSuggestions(words, prefix); })
        .catch(function () { closeSuggestions(); });
    }, 120);
  });

  // keyboard navigation in dropdown
  input.addEventListener('keydown', function (e) {
    var items = suggestBox.querySelectorAll('.suggest-item');
    if (!items.length) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      activeIdx = Math.min(activeIdx + 1, items.length - 1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      activeIdx = Math.max(activeIdx - 1, -1);
    } else if (e.key === 'Escape') {
      closeSuggestions(); return;
    } else if (e.key === 'Enter' && activeIdx >= 0) {
      e.preventDefault();
      input.value = currentSuggestions[activeIdx];
      sync && sync();
      closeSuggestions();
      input.form && input.form.submit();
      return;
    } else { return; }
    items.forEach(function (item, i) {
      item.classList.toggle('active', i === activeIdx);
    });
    if (activeIdx >= 0) input.value = currentSuggestions[activeIdx];
  });

  document.addEventListener('click', function (e) {
    if (!suggestBox.contains(e.target) && e.target !== input) closeSuggestions();
  });
})();

// ── Infinite scroll ──────────────────────────────────────────────────────
function loadMore() {
  var hidden = document.querySelectorAll('.result--hidden');
  var count = 0;
  hidden.forEach(function (el) {
    if (count < 10) { el.classList.remove('result--hidden'); count++; }
  });
  if (!document.querySelector('.result--hidden')) {
    var wrap = document.getElementById('loadMoreWrap');
    if (wrap) wrap.style.display = 'none';
  }
}

(function () {
  var ticking = false;
  window.addEventListener('scroll', function () {
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(function () {
      var wrap = document.getElementById('loadMoreWrap');
      if (wrap && wrap.style.display !== 'none') {
        var rect = wrap.getBoundingClientRect();
        if (rect.top < window.innerHeight + 200) loadMore();
      }
      ticking = false;
    });
  });
})();
