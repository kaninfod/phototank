
class PhotoRotate < AppJob
  include Resque::Plugins::UniqueJob
  queue_as :utility

  def perform(photo_id, degrees)
    pf = PhotoFilesApi::Api::new
    begin
      photo = Photo.find(photo_id)
      new_phash = 0
      photo.get_photofiles_hash.each do |key, id|
        pf.rotate(id, degrees)
        if key == :original
          new_phash = pf.phash(id)
        end
      end

      #set and save phash
      photo.update(phash:new_phash, status: 0)
    rescue Exception => e
      @job_db.update(job_error: e, status: 2, completed_at: Time.now)
      Rails.logger.warn "Error raised on job id: #{@job_db.id}. Error: #{e}"
      return
    end

  end
end
