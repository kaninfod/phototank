import { Controller } from "@hotwired/stimulus"
import { setRating, addTags, removeTags, addNewTag } from '../services/backend_calls.js'

export default class extends Controller {
    static targets = ["selectPhoto", "ratePhoto", "offcanvasSelectedCount", "offcanvas", "thumbTemplate", "thumbGrid", "newTagForm", "selectedTag", "photoGrid", "topSentinel", "bottomSentinel", "photoTile"] 
    
    connect() {
        this.isInitialLoad = true;
        this.activeTileIndex = null
        this.createObserver()
        this.offcanvas = bootstrap.Offcanvas.getOrCreateInstance(this.offcanvasTarget)
        
        window.addEventListener('toast-action', (e) => this.handleToastAction(e.detail))
        
        // Keyboard navigation
        this.handleKeydown = this.handleKeydown.bind(this)
        window.addEventListener('keydown', this.handleKeydown)

        this.dispatch("request-initial-state")    
        this.extractQueryParams()
        this.captureGridGuids()
        requestAnimationFrame(() => {
            this.provideHeadroom();
        });
    }

    createObserver() {
        const options = {
            root: null, // use the viewport
            rootMargin: '200px', // start loading 200px before the user reaches the end
            threshold: 0.1
        }

        this.observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    if (entry.target === this.bottomSentinelTarget) {
                        console.log("Bottom sentinel intersecting, loading older photos")
                        this.queryParams = { ...this.queryParams, direction: "older" }
                        this.fetchMore()
                    } else if (entry.target === this.topSentinelTarget) {
                        console.log("Top sentinel intersecting, loading newer photos")
                        this.queryParams = { ...this.queryParams, direction: "newer" }
                        this.fetchMore()
                    }
                }
            })
            console.log("IntersectionObserver entries:", entries)
        }, options)

        this.observer.observe(this.bottomSentinelTarget)
        this.observer.observe(this.topSentinelTarget)
    }

    disconnect() {
        window.removeEventListener('keydown', this.handleKeydown)
    }

    provideHeadroom() {
        // We check the 'main' element (our scroll container)
        const container = document.querySelector('main'); 
        
        if (container && container.scrollTop === 0) {
            // We nudge it down. 70-100px is usually enough to 
            // push the top sentinel out of the intersection zone.
            container.scrollTop = 80; 
            
            // After a tiny delay, we allow 'newer' fetches to trigger
            setTimeout(() => {
                this.isInitialLoad = false;
            }, 100);
        }
    }

    extractQueryParams() {
        if (this.hasPhotoGridTarget) {
            const params = {};
            Object.keys(this.photoGridTarget.dataset).forEach(key => {
                if (key.startsWith('queryParams')) {
                    // This will give you keys like: jumpDate, limit, rating, etc.
                    const paramName = key.replace('queryParams', '').toLowerCase();
                    params[paramName] = this.photoGridTarget.dataset[key];
                }
            });
            this.queryParams = params
            console.log("Extracted query params from photo grid dataset:", this.queryParams)
        }    
    }

    async fetchMore() {
        const queryParams = this.queryParams
        const queryString = new URLSearchParams(queryParams).toString();
        const url = `/phototank/?${queryString}`;
        const direction = queryParams.direction;
        const container = document.querySelector('main')
        
        try {
            const response = await fetch(url, {
                headers: {
                    "X-Requested-With": "XMLHttpRequest"
                }
            });
            if (!response.ok) throw new Error("Network response was ok");
            
            const html = await response.text();

            if (direction === "newer") {
                // Save reference to first child before inserting
                const firstChildBefore = this.photoGridTarget.firstElementChild
                const scrollBefore = container.scrollTop
                
                // Insert at top
                this.photoGridTarget.insertAdjacentHTML('afterbegin', html)
                
                // Scroll the old first child back into view (this maintains user position)
                if (firstChildBefore) {
                    firstChildBefore.scrollIntoView({ block: 'start' })
                    console.log(`Inserted newer photos, scrolled back to reference element`)
                }
            } else {
                this.photoGridTarget.insertAdjacentHTML('beforeend', html);
            }

            this.queryParams = { ...this.queryParams, older: response.headers.get("older_cursor"), newer: response.headers.get("newer_cursor") }
            console.log("Updated query params after fetching more:", this.queryParams)
            
            // Capture the updated grid GUIDs (includes both old and new)
            this.captureGridGuids()

        } catch (error) {
            console.error("Could not fetch more photos:", error);
        }
    }

    refreshUI(event) {
        console.log("Refreshing gallery UI with new selection:", event.detail)
        const { selected } = event.detail
        this.selectedGuids = selected
        this.loadThumbsToCanvas(selected)
        this.initSelection(selected)
        
        if (this.hasOffcanvasSelectedCountTarget) {
            this.offcanvasSelectedCountTarget.textContent = `Selected: ${selected.length}`
        }

        
    }    

    togglePhotoSelection(event) {
        const guid = event.currentTarget.dataset.guid
        window.dispatchEvent(new CustomEvent('update-selected-photos', { detail: { guid, action: "toggle" } }))
        
        window.dispatchEvent(new CustomEvent('show-toast', {
            detail: {
                title: "Selected",
                message: "Photo added to selection.",
                type: "success",
                delay: 1000
            }
        }))
    }

    removeSelectedPhoto(guid) {
        window.dispatchEvent(new CustomEvent('update-selected-photos', { detail: { guid, action: "remove" } }))
    }   

    addSelectedPhoto(guid) {
        window.dispatchEvent(new CustomEvent('update-selected-photos', { detail: { guid, action: "add" } }))
    }   

    clearSelected() {
        console.log("Clearing selected photos")
        window.dispatchEvent(new CustomEvent('update-selected-photos', { detail: { guid: null, action: "clear" } }))

        window.dispatchEvent(new CustomEvent('show-toast', {
            detail: {
                title: "Selected",
                message: "Photo selection cleared.",
                type: "success",
                delay: 3000
            }
        }))
    }

    initSelection(selectedGuids) {
        const savedGuids = selectedGuids
        
        this.selectPhotoTargets.forEach(checkbox => {            
            if (savedGuids.includes(checkbox.dataset.guid)) {
                checkbox.checked = true
            } else {
                checkbox.checked = false
            }
        })
    }


    toggleOffcanvas() {
        console.log("Toggling offcanvas from Gallery controller")
        const offcanvas = this.offcanvas
        if (this.offcanvas._isShown) {
            console.log("Hiding offcanvas")
            this.offcanvas.hide()
        } else {
            console.log("Showing offcanvas")

            this.offcanvas.show()
            this.loadThumbsToCanvas(this.selectedGuids)
        }
    }


    loadThumbsToCanvas(guids) {        
        console.log("loading thumbs for selected photos:")

        if (!this.offcanvas._isShown) { return }

        const content = this.thumbTemplateTarget
        this.thumbGridTarget.innerHTML = ""
        this.thumbGridTarget.appendChild(content)
        guids.forEach(guid => {
            // Clone the template content
            const newthumb = content.cloneNode(true)
            
            const link = newthumb.querySelector('a')
            const img = newthumb.querySelector('img')
            const btn = newthumb.querySelector('button')
            newthumb.classList.remove("d-none")
            link.href = `/phototank/photo/${guid}` 
            img.src = `/phototank/thumb/${guid}`
            img.alt = guid
            btn.dataset.guid = guid

            this.thumbGridTarget.appendChild(newthumb)
        })

    }

    async updatePhotoRating(guid, newRating) {
        try {
            await setRating(guid, newRating)
            
            // Find and update the rating button
            const button = this.ratePhotoTargets.find(btn => btn.dataset.guid === guid)
            if (button) {
                button.classList.add("bg-warning", "text-dark")
                button.classList.remove("bg-secondary", "text-light")
                button.textContent = String(newRating)
                button.dataset.rating = String(newRating)
                console.log(`Rating for photo ${guid} updated successfully to ${newRating}`)
            }
            
            window.dispatchEvent(new CustomEvent('show-toast', {
                detail: {
                    title: "Rated",
                    message: "Photo rating updated.",
                    type: "success",
                    delay: 1000
                }
            }))
        } catch (error) {
            console.error("Error updating rating:", error)
        }
    }

    async changePhotoRating(event) {
        const button = event.target
        const guid = button.dataset.guid
        const oldRating = parseInt(button.dataset.rating, 10) || 0
        const newRating = oldRating < 3 ? oldRating + 1 : 0
        await this.updatePhotoRating(guid, newRating)
    }

    async rateActiveTile() {
        const tile = this.getActiveTile()
        if (!tile) return
        
        const guid = tile.dataset.guid
        const button = this.ratePhotoTargets.find(btn => btn.dataset.guid === guid)
        const oldRating = button ? parseInt(button.dataset.rating, 10) || 0 : 0
        const newRating = oldRating < 3 ? oldRating + 1 : 0
        await this.updatePhotoRating(guid, newRating)
    }


    async addTagsToSelected() {
        const selectedTag = this.selectedTagTarget.value
        console.log(`Adding ${selectedTag} to selected photos:`, this.selectedGuids)
        
        if  (!selectedTag) {
            console.warn("No tag selected to apply")
            return
        } 
        try {
            await addTags(this.selectedGuids, selectedTag)
            console.log("Tag added successfully")
        } catch (error) {
            console.error("Error updating tags:", error)
            
        }

        window.dispatchEvent(new CustomEvent('show-toast', {
            detail: {
                title: "Added Tag",
                message: "Tag added to selected photos.",
                type: "success",
                delay: 3000
            }
        }))        
    }

    async removeTagsFromSelected() {
        console.log("Removing tags from selected photos:", this.selectedGuids)
        
        const selectedTag = this.selectedTagTarget.value
        
        try {
            await removeTags(this.selectedGuids, selectedTag)
            console.log("Tag removed successfully:", this.selectedGuids)
        } catch (error) {
            console.error("Error updating tags:", error)
            
        }
        window.dispatchEvent(new CustomEvent('show-toast', {
            detail: {
                title: "Removed Tag",
                message: "Tag removed from selected photos.",
                type: "success",
                delay: 3000
            }
        }))        
    }
    
    async addNewTag() {
        console.log("Adding new tag and applying to selected photos:", this.selectedGuids)
        if (!this.hasNewTagFormTarget) { return }
        
        const nameInput = this.newTagFormTarget.querySelector("#newTagName")
        const descInput = this.newTagFormTarget.querySelector("#newTagDesc")
        const colorSelect = this.newTagFormTarget.querySelector("#newTagColor")

        console.log("GUID for adding tags:", nameInput.value, descInput.value, colorSelect.value)

        const payload = {
            name: nameInput.value,
            description: descInput.value,
            color: colorSelect.value
        }

        try {
            await addNewTag(payload)
            console.log("Tag created successfully:", payload)
        } catch (error) {
            console.error("Error creating tag:", error)
        }

    }

    deleteSelected(event) {
        console.log("Deleting selected photos:", this.selectedGuids)

        window.dispatchEvent(new CustomEvent('show-toast', {
            detail: {
                title: "Confirm Delete",
                message: `Delete ${this.selectedGuids.length} photos? This cannot be undone.`,
                type: "danger",
                actions: [
                    { label: "Delete", action: "confirm-delete", class: "btn-danger" },
                    { label: "Cancel", action: "cancel", class: "btn-secondary" }
                ]
            }
        }))
    }

    performDelete() {
        console.log("Performing delete for selected photos:", this.selectedGuids)
        const url = '/phototank/delete'
        const payload = { guids: this.selectedGuids }
        this.serverRequest(url, 'POST', payload)
        .then(payload => {
            console.log("Photos deleted successfully:", payload)
            
        }).catch(error => {
            console.error("Error deleting photos:", error)
        }) 
    }

    handleToastAction(detail) {
        const { action } = detail
        if (action === 'confirm-delete') {
            this.performDelete()
        }
    }


    async serverRequest(url, type, payload) {
        // const rateUrl = '/phototank/tags'
        const resp = await fetch(url, {
            method: type,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });

        let responsePayload = null;
        try { 
            responsePayload = await resp.json(); 
        } catch { 
            console.error('Failed to parse response JSON for request to ', url);
        }


        if (!resp.ok) {
            console.error(`Error with request to ${url}: ${resp.statusText}`);
            throw new Error(resp.statusText);
        } else {
            console.log(`Request to ${url} successful`, responsePayload);
            return responsePayload
        }
    }

    // ========== Keyboard Navigation ==========
    
    #directionMap = {
        'ArrowLeft': 'left',
        'ArrowRight': 'right',
        'ArrowUp': 'up',
        'ArrowDown': 'down'
    }

    handleKeydown(e) {
        // Handle R key to rate active tile (but not Cmd+R, Ctrl+R, etc.)
        if (e.key === 'r' || e.key === 'R') {
            if (e.metaKey || e.ctrlKey || e.altKey || e.shiftKey) return
            e.preventDefault()
            this.rateActiveTile()
            return
        }

        // Handle Space to toggle selection on active tile
        if (e.key === ' ') {
            e.preventDefault()
            const tile = this.getActiveTile()
            if (tile) this.toggleTileSelection(tile)
            return
        }

        // Handle Enter to open active tile in detail view
        if (e.key === 'Enter') {
            e.preventDefault()
            const guid = this.getActiveTileGuid()
            if (guid) {
                const queryString = new URLSearchParams(this.queryParams).toString()
                window.location.href = `/phototank/photo/${guid}?${queryString}`
            }
            return
        }

        // Handle arrow keys for navigation
        const direction = this.#directionMap[e.key]
        if (!direction || this.photoTileTargets.length === 0) return

        e.preventDefault()
        this.moveInGrid(direction)
    }

    getActiveTileGuid() {
        const tile = this.getActiveTile()
        return tile ? tile.dataset.guid : null
    }

    toggleTileSelection(tile) {
        const guid = tile.dataset.guid
        const checkbox = this.selectPhotoTargets.find(cb => cb.dataset.guid === guid)
        if (!checkbox) return

        checkbox.checked = !checkbox.checked
        if (checkbox.checked) {
            this.addSelectedPhoto(guid)
        } else {
            this.removeSelectedPhoto(guid)
        }
    }

    getActiveTile() {
        if (this.activeTileIndex !== null && this.photoTileTargets[this.activeTileIndex]) {
            return this.photoTileTargets[this.activeTileIndex]
        }
        return null
    }

    setActiveTile(tile) {
        // Remove active class from previous tile
        if (this.activeTileIndex !== null && this.photoTileTargets[this.activeTileIndex]) {
            this.photoTileTargets[this.activeTileIndex].classList.remove('kb-active')
        }

        // Set new active tile
        const newIndex = this.photoTileTargets.indexOf(tile)
        if (newIndex !== -1) {
            this.activeTileIndex = newIndex
            tile.classList.add('kb-active')
            tile.scrollIntoView({ block: 'nearest', inline: 'nearest' })
        }
    }

    getTileCenter(tile) {
        const rect = tile.getBoundingClientRect()
        return {
            x: rect.left + rect.width / 2,
            y: rect.top + rect.height / 2
        }
    }

    moveSpatial(currentTile, direction) {
        const currentCenter = this.getTileCenter(currentTile)
        let bestTile = null
        let bestScore = Infinity

        for (const tile of this.photoTileTargets) {
            if (tile === currentTile) continue

            const tileCenter = this.getTileCenter(tile)
            const dx = tileCenter.x - currentCenter.x
            const dy = tileCenter.y - currentCenter.y

            // Filter by direction
            if (direction === 'left' && dx >= -1) continue
            if (direction === 'right' && dx <= 1) continue
            if (direction === 'up' && dy >= -1) continue
            if (direction === 'down' && dy <= 1) continue

            // Weight y-distance more for left/right, x-distance more for up/down
            const weight = (direction === 'left' || direction === 'right') ? 2.5 : 2.0
            const score = (dx * dx) + (dy * weight) * (dy * weight)

            if (score < bestScore) {
                bestScore = score
                bestTile = tile
            }
        }

        return bestTile
    }

    moveInGrid(direction) {
        if (this.photoTileTargets.length === 0) return

        let active = this.getActiveTile()
        
        if (!active) {
            // No active tile, activate the first one
            this.setActiveTile(this.photoTileTargets[0])
            return
        }

        const next = this.moveSpatial(active, direction)
        if (next) {
            this.setActiveTile(next)
        }
    }

    captureGridGuids() {
        const guids = this.photoTileTargets.map(tile => tile.dataset.guid)
        window.dispatchEvent(new CustomEvent('update-grid-guids', { detail: { gridGuids: guids } }))
        console.log("Grid GUIDs captured and dispatched:", guids.length, "photos")
    }

}