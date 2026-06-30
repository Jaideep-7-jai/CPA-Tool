/**
 * CPA Tool – New Request Form Handler
 * Handles:
 *   - Doordash auto-fill (client, criteria, comp type, channel)
 *   - Request name uniqueness check (debounced)
 *   - Criteria value field show/hide
 *   - File upload field show/hide
 *   - Form submission via /api/submit
 */

document.addEventListener('DOMContentLoaded', () => {
  const form            = document.getElementById('requestForm');
  const formMessage     = document.getElementById('formMessage');

  // Fields
  const requestNameEl   = document.getElementById('request_name');
  const nameStatusEl    = document.getElementById('nameStatus');
  const requestTypeEl   = document.getElementById('request_type');
  const clientNameEl    = document.getElementById('client_name');
  const criteriaTypeEl  = document.getElementById('criteria_type');
  const compTypeEl      = document.getElementById('comp_type');
  const channelEl       = document.getElementById('channel');

  // Conditional groups
  const criteriaValueGroup = document.getElementById('criteriaValueGroup');
  const criteriaValueEl    = document.getElementById('criteria_value');
  const criteriaValueLabel = document.getElementById('criteriaValueLabel');
  const criteriaValueHint  = document.getElementById('criteriaValueHint');
  const fileUploadGroup    = document.getElementById('fileUploadGroup');
  const compTypeGroup      = document.getElementById('compTypeGroup');

  // ── Doordash auto-fill ─────────────────────────────────────────────────────
  function applyDoordashDefaults() {
    const isDoordash = requestTypeEl.value === 'Doordash';

    if (isDoordash) {
      clientNameEl.value  = 'Doordash';
      clientNameEl.setAttribute('readonly', true);
      criteriaTypeEl.value = 'zips';
      criteriaTypeEl.setAttribute('disabled', true);
      compTypeEl.value    = 'include';
      compTypeEl.setAttribute('disabled', true);
      channelEl.value     = 'ALL';
      channelEl.setAttribute('disabled', true);
    } else {
      clientNameEl.removeAttribute('readonly');
      if (clientNameEl.value === 'Doordash') clientNameEl.value = '';
      criteriaTypeEl.removeAttribute('disabled');
      compTypeEl.removeAttribute('disabled');
      channelEl.removeAttribute('disabled');
    }
    updateCriteriaFields();
  }

  // ── Show/hide criteria value + file upload ─────────────────────────────────
  function updateCriteriaFields() {
    const criteria     = criteriaTypeEl.value;
    const isZipsOrDD   = criteria === 'zips';
    const isAge        = criteria === 'age';
    const isState      = criteria === 'state';

    // Criteria value field: show only for age/state
    if (isZipsOrDD) {
      criteriaValueGroup.classList.add('hidden');
      criteriaValueEl.removeAttribute('required');
      criteriaValueEl.value = '';
    } else {
      criteriaValueGroup.classList.remove('hidden');
      criteriaValueEl.setAttribute('required', true);
      if (isAge) {
        criteriaValueLabel.textContent = 'Age Value';
        criteriaValueEl.placeholder    = 'e.g. 55';
        criteriaValueHint.textContent  = 'Enter a single age number.';
        criteriaValueEl.type           = 'number';
      } else {
        criteriaValueLabel.textContent = 'State Codes';
        criteriaValueEl.placeholder    = 'e.g. CA,TX,NY';
        criteriaValueHint.textContent  = 'Comma-separated state codes (e.g. CA, TX).';
        criteriaValueEl.type           = 'text';
      }
    }

    // File upload: show for zips/doordash
    if (isZipsOrDD) {
      fileUploadGroup.classList.remove('hidden');
    } else {
      fileUploadGroup.classList.add('hidden');
    }

    // Comp type options: age uses greater/less; others use include/exclude
    updateCompTypeOptions(criteria);
  }

  function updateCompTypeOptions(criteria) {
    const isAge = criteria === 'age';
    // Clear and rebuild
    compTypeEl.innerHTML = '';
    if (isAge) {
      compTypeEl.innerHTML = `
        <option value="greater">Greater Than or Equal To</option>
        <option value="less">Less Than</option>
      `;
    } else {
      compTypeEl.innerHTML = `
        <option value="include">Include</option>
        <option value="exclude">Exclude</option>
      `;
    }
    // Re-apply Doordash lock after rebuild
    if (requestTypeEl.value === 'Doordash') {
      compTypeEl.value = 'include';
      compTypeEl.setAttribute('disabled', true);
    }
  }

  // ── Request name uniqueness check (debounced) ──────────────────────────────
  let nameCheckTimer = null;
  let nameIsValid    = false;

  function checkRequestName(name) {
    if (!name) {
      nameStatusEl.textContent = '';
      nameStatusEl.className   = 'name-status';
      nameIsValid = false;
      return;
    }
    nameStatusEl.textContent = 'Checking…';
    nameStatusEl.className   = 'name-status checking';

    fetch(`/api/check-name?name=${encodeURIComponent(name)}`)
      .then(r => r.json())
      .then(data => {
        if (data.available) {
          nameStatusEl.textContent = '✅ Available';
          nameStatusEl.className   = 'name-status available';
          nameIsValid = true;
        } else {
          nameStatusEl.textContent = '❌ Already taken';
          nameStatusEl.className   = 'name-status taken';
          nameIsValid = false;
        }
      })
      .catch(() => {
        nameStatusEl.textContent = '⚠ Check failed';
        nameStatusEl.className   = 'name-status error';
        nameIsValid = false;
      });
  }

  requestNameEl.addEventListener('input', () => {
    clearTimeout(nameCheckTimer);
    nameIsValid = false;
    nameCheckTimer = setTimeout(() => checkRequestName(requestNameEl.value.trim()), 500);
  });

  // ── Event listeners ────────────────────────────────────────────────────────
  requestTypeEl.addEventListener('change', applyDoordashDefaults);
  criteriaTypeEl.addEventListener('change', updateCriteriaFields);

  // Drag & drop styling
  const dropArea = document.getElementById('fileDropArea');
  if (dropArea) {
    dropArea.addEventListener('dragover',  e => { e.preventDefault(); dropArea.classList.add('drag-over'); });
    dropArea.addEventListener('dragleave', () => dropArea.classList.remove('drag-over'));
    dropArea.addEventListener('drop', e => {
      e.preventDefault();
      dropArea.classList.remove('drag-over');
      const file = e.dataTransfer.files[0];
      if (file) {
        document.getElementById('zip_file').files = e.dataTransfer.files;
        document.getElementById('fileLabel').textContent = file.name;
      }
    });
    document.getElementById('zip_file').addEventListener('change', e => {
      const file = e.target.files[0];
      if (file) document.getElementById('fileLabel').textContent = file.name;
    });
  }

  // ── Form submit ────────────────────────────────────────────────────────────
  if (form) {
    form.addEventListener('submit', async (e) => {
      e.preventDefault();

      if (!nameIsValid) {
        formMessage.textContent = '⚠ Please wait for the name check or fix the request name.';
        formMessage.className   = 'message error';
        return;
      }

      formMessage.textContent = 'Submitting…';
      formMessage.className   = 'message loading';

      // Re-enable disabled selects so their values are included in FormData
      const disabledSelects = form.querySelectorAll('select[disabled]');
      disabledSelects.forEach(s => s.removeAttribute('disabled'));

      const body = new FormData(form);

      // Re-disable after collecting
      if (requestTypeEl.value === 'Doordash') {
        criteriaTypeEl.setAttribute('disabled', true);
        compTypeEl.setAttribute('disabled', true);
        channelEl.setAttribute('disabled', true);
      }

      try {
        const res  = await fetch('/api/submit', { method: 'POST', body });
        const data = await res.json();

        if (!res.ok) {
          formMessage.textContent = data.error || 'Submission failed';
          formMessage.className   = 'message error';
          return;
        }

        form.reset();
        applyDoordashDefaults();
        updateCriteriaFields();
        nameIsValid = false;
        nameStatusEl.textContent = '';

        formMessage.textContent = `✅ Request "${data.request_name}" submitted successfully!`;
        formMessage.className   = 'message success';

        setTimeout(() => location.reload(), 2000);
      } catch (err) {
        formMessage.textContent = `Error: ${err.message}`;
        formMessage.className   = 'message error';
      }
    });
  }

  // ── Init ───────────────────────────────────────────────────────────────────
  applyDoordashDefaults();
  updateCriteriaFields();
});
