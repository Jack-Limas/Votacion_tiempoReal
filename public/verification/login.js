import { $, getSession, saveToken } from './console.js';

const pages = { auditor: 'auditor.html', coordinator: 'coordinator.html' };
const existing = getSession();
if (existing) location.replace(pages[existing.role]);

let role = 'auditor';
for (const button of document.querySelectorAll('.segmented button')) {
  button.addEventListener('click', () => {
    role = button.dataset.role;
    for (const other of document.querySelectorAll('.segmented button')) {
      other.classList.toggle('active', other === button);
      other.setAttribute('aria-selected', String(other === button));
    }
    $('input[name=id]').placeholder = role === 'auditor' ? 'AUD-001' : 'COORD-01';
  });
}

$('#login').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = new FormData(event.target);
  const submit = $('#submit');
  submit.disabled = true;
  $('#error').textContent = '';
  try {
    const response = await fetch('/api/verification/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: form.get('id').trim(),
        name: form.get('name').trim() || undefined,
        role,
        accessCode: form.get('accessCode')
      })
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      $('#error').textContent = response.status === 429
        ? `Too many attempts. Try again in ${body.retryAfterSeconds ?? 'a few'} s.`
        : body.message ?? `Sign-in failed (${response.status}).`;
      return;
    }
    saveToken(body.token);
    location.href = pages[body.role];
  } catch {
    $('#error').textContent = 'Network error. Check your connection and try again.';
  } finally {
    submit.disabled = false;
  }
});
