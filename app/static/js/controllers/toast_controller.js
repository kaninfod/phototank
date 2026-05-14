import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["container"]
    
    connect() {
        window.addEventListener("show-toast", (e) => this.showToast(e.detail))
    }
    
    disconnect() {
        window.removeEventListener("show-toast", this.showToast.bind(this))
    }
    
    showToast(options) {
        const {
            title = "Notification",
            message = "",
            type = "info",
            actions = [],
            autohide = true,
            delay = 5000
        } = options
        
        const toastId = `toast-${Date.now()}`
        
        // Clone template
        const template = document.getElementById("toastTemplate")
        const toastEl = template.content.cloneNode(true)
        
        // Set content
        const toast = toastEl.querySelector('.toast')
        toast.id = toastId
        
        const header = toastEl.querySelector('.toast-header')
        header.className = `toast-header bg-${type === 'danger' ? 'danger' : type} text-white`
        
        toastEl.querySelector('.toast-title').textContent = title
        toastEl.querySelector('.toast-message').textContent = message
        
        // Add actions
        const actionsDiv = toastEl.querySelector('.toast-actions')
        if (actions.length > 0) {
            actions.forEach(a => {
                const btn = document.createElement('button')
                btn.className = `btn btn-sm ${a.class || 'btn-primary'}`
                btn.textContent = a.label
                btn.dataset.action = a.action
                btn.addEventListener('click', (e) => {
                    window.dispatchEvent(new CustomEvent('toast-action', {
                        detail: { action: a.action, toastId }
                    }))
                    bootstrap.Toast.getInstance(toast).hide()
                })
                actionsDiv.appendChild(btn)
            })
        } else {
            actionsDiv.remove()
        }
        
        // Add to container and show
        this.containerTarget.appendChild(toastEl)
        const bsToast = new bootstrap.Toast(toast, {
            autohide: autohide && actions.length === 0,
            delay: delay
        })
        bsToast.show()
    }
}