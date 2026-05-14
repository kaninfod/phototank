import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    // These names map to 'data-photo-target' in the HTML
    static targets = ["selectedCount"] 
    static values = { 
    }


    connect() {
        console.log("Navbar controller connected")
    }

    refreshUI(event) {
        console.log("Navbar controller received UI refresh event:", event.detail)
        const { selected } = event.detail

        if (this.hasSelectedCountTarget) {
            this.selectedCountTarget.textContent = selected.length
        }
    }

    handleToggleOffcanvas() {
        console.log("Toggling offcanvas from Navbar controller")
        window.dispatchEvent(new CustomEvent("toggle-offcanvas"))
    }


}