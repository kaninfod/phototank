import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    // These names map to 'data-photo-target' in the HTML
    static targets = ["loadOlderButton", "loadNewerButton"] 
    static values = { 
        appState: Object,
        phototankStateKey: String 
    }
    
    initialize() {  
        const saved = localStorage.getItem(this.phototankStateKeyValue)
        const state = { 
            queryParams: {}, 
            selected: [],
            gridGuids: []
        }

        this.appStateValue = saved ? JSON.parse(saved) : state
    }

    appStateValueChanged(value) {
        localStorage.setItem(this.phototankStateKeyValue, JSON.stringify(value))
        this.dispatch("updated", { detail: value })
        window.dispatchEvent(new CustomEvent('appstate:updated', { detail: value }))
        console.log("AppState updated:", value, this.phototankStateKeyValue)
    }

    connect() {
        this.dispatch("updated", { detail: this.appStateValue })
        window.dispatchEvent(new CustomEvent('appstate:updated', { detail: this.appStateValue }))
        
        // Listen for selection updates from gallery/photodetail controllers (window events)
        window.addEventListener('update-selected-photos', (e) => this.updateSelection(e))
        
        // Listen for grid guid updates from gallery controller
        window.addEventListener('update-grid-guids', (e) => this.updateGridGuids(e))
        
        // Listen for requests for state (from photodetail controller)
        window.addEventListener('request-app-state', () => {
            window.dispatchEvent(new CustomEvent('appstate:updated', { detail: this.appStateValue }))
        })
    }

    sendStateRequest() {
        this.dispatch("updated", { detail: this.appStateValue })
    }

    updateSelection(event) {
        const { guid, action } = event.detail
        let selected = [...this.appStateValue.selected]

        if (action === "add") {
            if (!selected.includes(guid)) selected.push(guid)
        } else if (action === "remove") {
            selected = selected.filter(id => id !== guid)
        } else if (action === "clear") {
            selected = []
        } else if (action === "toggle") {
            if (selected.includes(guid)) {
                selected = selected.filter(id => id !== guid)
            } else {
                selected.push(guid)
            }
        }   

        this.appStateValue = { ...this.appStateValue, selected }
    }

    updateGridGuids(event) {
        const { gridGuids } = event.detail
        this.appStateValue = { ...this.appStateValue, gridGuids }
    }


 

}
