// ===== request-form.js =====
(function () {
  const requestTypeEl   = document.getElementById('request_type');
  const clientNameEl    = document.getElementById('client_name');
  const criteriaTypeEl  = document.getElementById('criteria_type');
  const criteriaValGrp  = document.getElementById('criteriaValueGroup');
  const criteriaValEl   = document.getElementById('criteria_value');
  const criteriaValLabel= document.getElementById('criteriaValueLabel');
  const criteriaValHint = document.getElementById('criteriaValueHint');
  const compTypeEl      = document.getElementById('comp_type');
  const compTypeGrp     = document.getElementById('compTypeGroup');
  const channelEl       = document.getElementById('channel');
  const fileGrp         = document.getElementById('fileUploadGroup');
  const fileInput       = document.getElementById('zip_file');
  const fileLabelEl     = document.getElementById('fileLabel');
  const requestNameEl   = document.getElementById('request_name');
  const nameStatusEl    = document.getElementById('nameStatus');
  const form            = document.getElementById('requestForm');
  const msgEl           = document.getElementById('formMessage');

  // ---- Doordash / Type logic ----
  function applyTypeRules() {
    const type = requestTypeEl.value;
    if (type === 'Doordash') {
      clientNameEl.value    = 'Doordash';
      clientNameEl.readOnly = true;
      criteriaTypeEl.value  = 'zips';
      criteriaTypeEl.disabled = true;
      compTypeEl.value      = 'include';
      channelEl.value       = 'ALL';
      applyCriteriaRules();
    } else {
      clientNameEl.readOnly = false;
      criteriaTypeEl.disabled = false;
    }
  }

  // ---- Criteria logic ----
  function applyCriteriaRules() {
    const type     = requestTypeEl.value;
    const criteria = criteriaTypeEl.value;

    // Show/hide criteria value field
    if (criteria === 'age') {
      criteriaValGrp.classList.remove('hidden');
      criteriaValLabel.textContent = 'Age Value';
      criteriaValHint.textContent  = 'e.g. 55';
      criteriaValEl.placeholder   = '55';
      criteriaValEl.required      = true;
      // Comp options for age
      setCompOptions([
        { value: 'greater', label: 'Greater Than or Equal To' },
        { value: 'less',    label: 'Less Than' }
      ]);
      fileGrp.classList.add('hidden');
      fileInput.required = false;
    } else if (criteria === 'state') {
      criteriaValGrp.classList.remove('hidden');
      criteriaValLabel.textContent = 'State(s)';
      criteriaValHint.textContent  = 'Comma-separated state codes, e.g. CA,TX,NY';
      criteriaValEl.placeholder   = 'CA,TX,NY';
      criteriaValEl.required      = true;
      setCompOptions([
        { value: 'include', label: 'Include' },
        { value: 'exclude', label: 'Exclude' }
      ]);
      fileGrp.classList.add('hidden');
      fileInput.required = false;
    } else if (criteria === 'zips') {
      criteriaValGrp.classList.add('hidden');
      criteriaValEl.required = false;
      criteriaValEl.value    = '';
      setCompOptions(type === 'Doordash'
        ? [{ value: 'include', label: 'Include (Doordash)' }]
        : [
            { value: 'include', label: 'Include' },
            { value: 'exclude', label: 'Exclude' }
          ]
      );
      fileGrp.classList.remove('hidden');
      fileInput.required = true;
    } else {
      criteriaValGrp.classList.add('hidden');
      fileGrp.classList.add('hidden');
      fileInput.required = false;
    }

    // Lock comp for Doordash
    if (type === 'Doordash') {
      compTypeEl.value    = 'include';
      compTypeEl.disabled = true;
    } else {
      compTypeEl.disabled = false;
    }
  }

  function setCompOptions(options) {
    const current = compTypeEl.value;
    compTypeEl.innerHTML = '';
    options.forEach(o => {
      const opt = document.createElement('option');
      opt.value = o.value;
      opt.textContent = o.label;
      compTypeEl.appendChild(opt);
    });
    const match = options.find(o => o.value === current);
    if (match) compTypeEl.value = current;
  }

  // ---- File label ----
  fileInput && fileInput.addEventListener('change', () => {
    fileLabelEl.textContent = fileInput.files.length
      ? fileInput.files[0].name
      : 'Click to select or drag & drop a CSV/TXT file';
  });

  // Drag & drop highlight
  const dropArea = document.getElementById('fileDropArea');
  if (dropArea) {
    dropArea.addEventListener('dragover',  e => { e.preventDefault(); dropArea.classList.add('drag-over'); });
    dropArea.addEventListener('dragleave', () => dropArea.classList.remove('drag-over'));
    dropArea.addEventListener('drop',      e => { e.preventDefault(); dropArea.classList.remove('drag-over'); fileInput.files = e.dataTransfer.files; fileInput.dispatchEvent(new Event('change')); });
  }

  // ---- Unique name check ----
  let nameTimer;
  requestNameEl && requestNameEl.addEventListener('input', () => {
    clearTimeout(nameTimer);
    const val = requestNameEl.value.trim();
    if (!val) { nameStatusEl.textContent = ''; nameStatusEl.className = 'name-status'; return; }
    nameStatusEl.textContent = 'Checking...';
    nameStatusEl.className   = 'name-status checking';
    nameTimer = setTimeout(async () => {
      try {
        const res  = await fetch('/api/check-name?name=' + encodeURIComponent(val));
        const data = await res.json();
        if (data.available) {
          nameStatusEl.textContent = '✅ Available';
          nameStatusEl.className   = 'name-status ok';
        } else {
          nameStatusEl.textContent = '❌ Already taken';
          nameStatusEl.className   = 'name-status taken';
        }
      } catch { nameStatusEl.textContent = ''; nameStatusEl.className = 'name-status'; }
    }, 500);
  });

  // ---- Event listeners ----
  requestTypeEl  && requestTypeEl.addEventListener('change',  () => { applyTypeRules(); applyCriteriaRules(); });
  criteriaTypeEl && criteriaTypeEl.addEventListener('change', applyCriteriaRules);

  // Init
  applyTypeRules();
  applyCriteriaRules();

  // ---- Form submit ----
  form && form.addEventListener('submit', async (e) => {
    e.preventDefault();

    // Block if name taken
    if (nameStatusEl.classList.contains('taken')) {
      msgEl.textContent = 'Request name already exists. Choose a different name.';
      msgEl.className   = 'message error';
      return;
    }

    msgEl.textContent = 'Submitting...';
    msgEl.className   = 'message loading';

    const body = new FormData(form);
    // If criteria_type was disabled (Doordash), manually add value
    if (criteriaTypeEl.disabled) body.set('criteria_type', criteriaTypeEl.value);
    if (compTypeEl.disabled)     body.set('comp_type',     compTypeEl.value);

    try {
      const res  = await fetch('/api/submit', { method: 'POST', body });
      const data = await res.json();
      if (!res.ok) {
        msgEl.textContent = data.error || 'Submission failed.';
        msgEl.className   = 'message error';
        return;
      }
      form.reset();
      nameStatusEl.textContent = '';
      fileLabelEl.textContent  = 'Click to select or drag & drop a CSV/TXT file';
      applyTypeRules();
      applyCriteriaRules();
      msgEl.textContent = '✅ Request "' + data.request_name + '" submitted successfully!';
      msgEl.className   = 'message success';
      setTimeout(() => location.reload(), 2000);
    } catch (err) {
      msgEl.textContent = 'Error: ' + err.message;
      msgEl.className   = 'message error';
    }
  });
})();
