import { Application } from "@hotwired/stimulus"
import galleryController from "./controllers/gallery_controller.js"
import photodetailController from "./controllers/photodetail_controller.js"
import appStateController from "./controllers/appState_controller.js"
import navbarController from "./controllers/navbar_controller.js"
import toastController from "./controllers/toast_controller.js"

const application = Application.start()

// 1. Enable Debugging (Logs all Stimulus activity to console)
application.debug = true

// 2. Register your controllers
// application.register("volume", VolumeController)
application.register("gallery", galleryController)
application.register("photodetail", photodetailController)
application.register("appstate", appStateController)
application.register("navbar", navbarController)
application.register("toast", toastController)
// 3. Optional: Global access for console debugging
window.Stimulus = application
