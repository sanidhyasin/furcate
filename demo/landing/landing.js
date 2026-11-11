// Copy buttons for the install commands. The page works without this script.
'use strict';
for (const btn of document.querySelectorAll('.copy')) {
  btn.addEventListener('click', async () => {
    const pre = btn.parentElement.querySelector('pre');
    try {
      await navigator.clipboard.writeText(pre.textContent);
      btn.textContent = 'Copied';
    } catch {
      btn.textContent = 'Select and copy';
    }
    setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
  });
}
