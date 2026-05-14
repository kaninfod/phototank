import { Controller } from "@hotwired/stimulus"
import { setRating, addTags, removeTags, addNewTag } from '../services/backend_calls.js'

export default class extends Controller {
    static targets = ["tagToAdd", "tagToClone", "tagContainer"] 
    static values = { guid: String, appState: Object }
    
    connect() {
        console.log("photodetail page loaded - current photo guid:", this.guidValue)
        this.handleKeydown = this.handleKeydown.bind(this)
        window.addEventListener('keydown', this.handleKeydown)
        
        // Initialize appState (will be populated by updateAppState method)
        this.appStateValue = { gridGuids: [] }
        
        // Request appState from appstate controller
        window.dispatchEvent(new CustomEvent('request-app-state'))
    }

    disconnect() {
        window.removeEventListener('keydown', this.handleKeydown)
    }

    updateAppState(event) {
        this.appStateValue = event.detail
        console.log("App state received with", this.appStateValue.gridGuids.length, "photos")
    }

    handleKeydown(e) {
        if (e.key === 'ArrowLeft') {
            e.preventDefault()
            this.navigatePhoto('prev')
        } else if (e.key === 'ArrowRight') {
            e.preventDefault()
            this.navigatePhoto('next')
        } else if (e.key === 'Escape') {
            e.preventDefault()
            this.backToGallery()
        } else if (e.key === ' ') {
            e.preventDefault()
            this.togglePhotoSelection()
        }
    }

    navigatePhoto(direction) {
        const gridGuids = this.appStateValue?.gridGuids || []
        if (gridGuids.length === 0) {
            console.warn("No grid context available for navigation")
            return
        }
        
        const currentIndex = gridGuids.indexOf(this.guidValue)
        if (currentIndex === -1) {
            console.warn("Current photo not in grid context")
            return
        }
        
        let nextIndex
        if (direction === 'next') {
            nextIndex = currentIndex + 1
            if (nextIndex >= gridGuids.length) {
                console.log("At end of grid")
                return
            }
        } else {
            nextIndex = currentIndex - 1
            if (nextIndex < 0) {
                console.log("At beginning of grid")
                return
            }
        }
        
        const nextGuid = gridGuids[nextIndex]
        window.location.href = `/phototank/photo/${nextGuid}?from=gallery`
    }

    backToGallery() {
        const queryParams = this.appStateValue?.queryParams || {}
        const queryString = new URLSearchParams(queryParams).toString()
        const url = `/phototank/?${queryString}`
        window.location.href = url
    }

    togglePhotoSelection() {
        window.dispatchEvent(new CustomEvent('update-selected-photos', { detail: { guid: this.guidValue, action: "toggle" } }))
        
        window.dispatchEvent(new CustomEvent('show-toast', {
            detail: {
                title: "Selected",
                message: "Photo added to selection.",
                type: "success",
                delay: 1000
            }
        }))
    }

    async removeTag(event) {
        console.log("Removing tags from photo:", this.guidValue)
        
        const tag = event.target.dataset.tagId
        console.log("Selected tag to remove:", tag)
        
        try {
            await removeTags([this.guidValue], tag)
            
            event.target.closest('span').remove()
            console.log("Tag removed successfully:", this.guidValue, tag)
        } catch (error) {
            console.error("Error updating tags:", error)
            
        }             
    }

    async addTag(event) {
        console.log("Adding tags to photo:", this.guidValue)
        const tag = this.tagToAddTarget.value
        console.log("Selected tag to add:", tag)
        
        try {
            const result = await addTags([this.guidValue], tag)
            
            const newtag = result.tag
            const tagClone = this.tagToCloneTarget.cloneNode(true)
            const button = tagClone.querySelector('button')
            
            tagClone.classList.remove("d-none")
            tagClone.className = `badge bg-${newtag.color} ${newtag.color === 'warning' ? 'text-dark' : 'text-light'}`
            tagClone.insertAdjacentHTML('afterbegin', newtag.name)
            tagClone.dataset.tagId = newtag.id
            
            button.dataset.tagId = newtag.id

            this.tagContainerTarget.appendChild(tagClone)
            console.log("Tag added successfully:", this.guidValue, newtag, tagClone)
        } catch (error) {
            console.error("Error updating tags:", error)
            
        }             
    }    
}