const criteriaEl = document.getElementById('criteria');
const compEl = document.getElementById('comp');
const ageWrap = document.getElementById('ageWrap');
const stateWrap = document.getElementById('stateWrap');
const zipWrap = document.getElementById('zipWrap');
const form = document.getElementById('requestForm');
const formMessage = document.getElementById('formMessage');
const requestList = document.getElementById('requestList');

function setCompOptions(criteria) {
  const options = criteria === 'age'
    ? [['greater', 'Greater than'], ['less', 'Less than']]
    : [['include', 'Include'], ['exclude', 'Exclude']];
  compEl.innerHTML = options.map(([v, t]) => `<option value="${v}">${t}</option>`).join('');
}

function toggleFields() {
  const criteria = criteriaEl.value;
  setCompOptions(criteria);
  ageWrap.classList.toggle('hidden', criteria !== 'age');
  stateWrap.classList.toggle('hidden', criteria !== 'state');
  zipWrap.classList.toggle('hidden', criteria !== 'zip');
  document.getElementById('age').required = criteria === 'age';
  document.getElementById('states').required = criteria === 'state';
  document.getElementById('zip_file').required = criteria === 'zip';
}

function renderItem(item) {
  const extra = item.criteria === 'age'
    ? `Age: ${item.age || '-'}`
    : item.criteria === 'state'
      ? `States: ${item.states || '-'}`
      : `ZIP: ${item.zip_file_path || '-'}`;
  return `
    <article class="request-item">
      <div class="row-between">
        <strong>${item.criteria.toUpperCase()}</strong>
        <span class="badge ${item.status}">${item.status}</span>
      </div>
      <div class="meta">Comp: ${item.comp}</div>
      <div class="meta">${extra}</div>
      <div class="meta">User: ${item.username}</div>
      <div class="meta">Output: ${item.output_dir}</div>
      <div class="meta">Created: ${item.created_at}</div>
    </article>`;
}

async function loadRequests() {
  const res = await fetch('/api/requests');
  const data = await res.json();
  requestList.innerHTML = data.items.length ? data.items.map(renderItem).join('') : "<p class='meta'>No requests yet.</p>";
}

if (form) {
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    formMessage.textContent = 'Submitting...';
    const body = new FormData(form);
    const res = await fetch('/api/submit', { method: 'POST', body });
    const data = await res.json();
    if (!res.ok) {
      formMessage.textContent = data.error || 'Submission failed';
      return;
    }
    form.reset();
    toggleFields();
    formMessage.textContent = `Request submitted: ${data.request_uuid}`;
    loadRequests();
  });
  criteriaEl.addEventListener('change', toggleFields);
  toggleFields();
  loadRequests();
  setInterval(loadRequests, 5000);
}
