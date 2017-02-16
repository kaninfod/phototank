class LocalImportSpawn < AppJob
  include Resque::Plugins::UniqueJob
  queue_as :import

  def perform(catalog_id)

    begin
      catalog = Catalog.find(catalog_id)
      catalog.not_synchronized.each do |instance|
        LocalImportPhotoJob.perform_later catalog_id, instance.photo_id
      end

    rescue Exception => e
      @job_db.update(job_error: e, status: 2, completed_at: Time.now)
      Rails.logger.warn "Error raised on job id: #{@job_db.id}. Error: #{e}"
      return
    end

  end
end
