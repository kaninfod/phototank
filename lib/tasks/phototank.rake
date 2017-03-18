namespace :phototank do
  desc "Create the Master catalog"
  task create_master_catalog: :environment do
    MasterCatalog.create_master
  end

  desc "add generic photos (eg missing)"
  task Add_generic_photo: :environment do
    pf = PhotoFilesApi::Api::new
    ['tm', 'md', 'lg'].each do |ext|
      image_path = File.join(Rails.root,'app','assets','images', "generic_#{ext}.jpg")
      response = pf.create image_path, nil, nil, 'generic_image'
      Setting["generic_image_#{ext}_id"] = response[:id]
    end

  end
  desc "Set the updating flag to false to allow updates"
  task master_not_updating: :environment do
    Catalog.master.settings.updating = false
  end
end
