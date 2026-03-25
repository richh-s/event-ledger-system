document.addEventListener('DOMContentLoaded', () => {
    fetchApplications();
    
    document.getElementById('btn-refresh').addEventListener('click', fetchApplications);
});

let currentAppId = null;

async function fetchApplications() {
    const list = document.getElementById('app-list');
    list.innerHTML = '<li class="app-item" style="text-align:center;">Loading ledger...</li>';
    
    try {
        const res = await fetch('/api/applications');
        const apps = await res.json();
        
        list.innerHTML = '';
        if (apps.length === 0) {
            list.innerHTML = '<li class="app-item">No applications found in the event store. Run snapshot tests first.</li>';
            return;
        }

        apps.forEach(app => {
            const li = document.createElement('li');
            li.className = 'app-item';
            // Formatting the date purely for UI viewing safely
            const dt = new Date(app.submitted_at).toLocaleString();
            
            li.innerHTML = `
                <div class="app-item-id">${app.id}</div>
                <div class="app-item-meta">${dt} &bull; ${app.correlation_id}</div>
            `;
            
            li.addEventListener('click', () => {
                document.querySelectorAll('.app-item').forEach(e => e.classList.remove('active'));
                li.classList.add('active');
                renderRegulatoryPackage(app.id);
            });
            
            list.appendChild(li);
        });

    } catch (e) {
        list.innerHTML = `<li class="app-item" style="color:red">Failed to connect to ledger API</li>`;
    }
}

async function renderRegulatoryPackage(id) {
    currentAppId = id;
    document.getElementById('empty-state').style.display = 'none';
    const view = document.getElementById('reconstruction-view');
    view.style.display = 'none';
    view.classList.remove('fade-in');
    
    try {
        const res = await fetch(`/api/applications/${id}/history`);
        if (!res.ok) throw new Error('Failed to fetch package data');
        
        const data = await res.json();
        view.style.display = 'block';
        
        // Let CSS animation run
        setTimeout(() => view.classList.add('fade-in'), 10);
        
        // Setup header
        document.getElementById('app-title').textContent = `Application: ${id}`;
        
        const val = data.validation;
        const bSchema = document.getElementById('val-badge-schema');
        if (val.schema_validation_passed) {
             bSchema.className = 'badge success';
             bSchema.textContent = 'Schema Validated';
        } else {
             bSchema.className = 'badge warning';
             bSchema.textContent = 'Schema Failed';
        }

        const audit = data.audit_integrity_section.integrity_passed;
        const bAudit = document.getElementById('val-badge-audit');
        if (audit === true) {
             bAudit.className = 'badge success';
             bAudit.textContent = 'Cryptographic Audit OK';
        } else {
             bAudit.className = 'badge warning';
             bAudit.textContent = 'Audit Needs Check';
        }

        // Stats Map
        const summary = data.reconstructed_read_model;
        
        document.getElementById('stat-state').textContent = summary.state.replace(/_/g, ' ');
        document.getElementById('stat-decision').textContent = summary.decision || 'PENDING';
        
        let amntForm = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(summary.requested_amount_usd || 0);
        document.getElementById('stat-amount').textContent = amntForm;
        
        document.getElementById('stat-filtered').textContent = val.invalid_events_filtered;

        // Narrative mapping
        document.getElementById('narrative-box').textContent = data.narrative;
        
        // Timeline Assembly
        const tlbox = document.getElementById('timeline-box');
        tlbox.innerHTML = '';
        data.timeline.forEach(t => {
             const dt = new Date(t.recorded_at).toLocaleTimeString();
             const el = document.createElement('div');
             el.className = 'timeline-item';
             el.innerHTML = `
                <div class="timeline-dot"></div>
                <div class="timeline-content">
                  <div class="timeline-meta">${dt} &bull; POS: ${t.global_position}</div>
                  <div style="font-weight:600; color:var(--text-main);">${t.event_type}</div>
                </div>
             `;
             tlbox.appendChild(el);
        });

        // JSON Dump Code Box
        document.getElementById('raw-package-box').textContent = JSON.stringify(data, null, 2);

        // Reset WhatIf
        document.getElementById('whatif-result').style.display = 'none';

    } catch(e) {
        alert(e.message);
    }
}

document.getElementById('btn-whatif').addEventListener('click', async () => {
    if(!currentAppId) return;
    
    const altModel = document.getElementById('whatif-model').value || 'Apex-Risk-V3';
    const btn = document.getElementById('btn-whatif');
    btn.textContent = 'Computing...';
    btn.disabled = true;

    try {
        const res = await fetch(`/api/applications/${currentAppId}/what-if`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({alternate_model: altModel})
        });
        const data = await res.json();
        
        const codebox = document.getElementById('whatif-code');
        const box = document.getElementById('whatif-result');
        codebox.textContent = JSON.stringify(data, null, 2);
        box.style.display = 'block';
        box.classList.add('fade-in');
        
    } catch(e) {
        alert("Simulation failed");
    } finally {
        btn.textContent = 'Simulate Hypothesis';
        btn.disabled = false;
    }
});
