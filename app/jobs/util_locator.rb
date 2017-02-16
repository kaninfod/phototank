class UtilLocator < AppJob
  include Resque::Plugins::UniqueJob
  queue_as :utility

  def perform(photo_id)

    begin

      @photo = Photo.find(photo_id)
      @photo.locate
      @photo.save
    rescue Exception => e
      @job_db.update(job_error: e, status: 2, completed_at: Time.now)
      Rails.logger.warn "Error raised on job id: #{@job_db.id}. Error: #{e}"
      return
    end

  end

end
