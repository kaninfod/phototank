class MasterImportPhoto < AppJob
  include Resque::Plugins::UniqueJob
  include PhotoFilesApi
  queue_as :import

  IMAGE_THUMB = '125x125'
  IMAGE_MEDIUM = '480x680'
  IMAGE_LARGE = '1024x1200'

  def perform(import_path, photo_id=false, import_mode=true)

    begin

      raise "File does not exist" unless File.exist?(import_path)
      data = import_flow(import_path, import_mode)

      #If a photo id was supplied then update that photo
      if not photo_id
        photo = Photo.new
      else
        photo = Photo.find(photo_id)
      end
      photo.update(data)
      instance = Instance.create(
        photo_id: photo.id,
        catalog_id: Catalog.master.id
      )

      UtilLocator.perform_later photo.id

    rescue Exception => e
      @job_db.update(job_error: e, status: 2, completed_at: Time.now)
      Rails.logger.warn "Error raised on job id: #{@job_db.id}. Error: #{e}"
      return
    end
  end


  def import_flow(path, import_mode=true)
    @data = {}

    @pf = PhotoFilesApi::Api::new
    #get phash and check if the photo already exists in the database
    phash = Phashion::Image.new(path)
    @data[:phash] = phash.fingerprint

    if Photo.exists(@data[:phash])
      _p = File.join(File.dirname(path), 'existing')
      FileUtils.mkdir_p _p
      FileUtils.mv path, _p
      raise "Photo already exists: #{path}"
    end

    #Create tempfiles for all photos to be created
    @org_file = Tempfile.new("org")
    @tm_file = Tempfile.new("thumb")
    @lg_file = Tempfile.new("large")
    @md_file = Tempfile.new("medium")

    begin
      #set the original file to the tempfile
      @org_file.write File.open(path).read ; :ok

      #Get metadata from photo
      @image = MiniMagick::Image.open(@org_file.path)
      @data[:original_width] = @image.width
      @data[:original_height] = @image.height
      @data[:file_size] = @image.size
      @data[:file_extension] = ".jpg"
      #Get EXIF data from photo
      exif = MiniExiftool.new(@org_file.path, opts={:numerical=>true})
      @data[:longitude] = exif.gpslongitude
      @data[:latitude] = exif.gpslatitude
      @data[:make] = exif.make
      @data[:model] = exif.model

      #Set data_taken; either from EXIF or from file timestamp
      if !exif.datetimeoriginal.blank?
        @data[:date_taken] = exif.datetimeoriginal
      elsif !exif.gpsdatestamp.blank? && !exif.gpstimestamp.blank?
        @data[:date_taken] = "#{exif.gpsdatestamp.gsub(':', '.')} #{exif.gpstimestamp}".to_datetime
      else
        exif.datetimeoriginal = @data[:date_taken] = File.ctime(@org_file.path)
      end
      exif["usercomment"] = "This photo is handled by PhotoTank as of #{DateTime.now.strftime("%Y.%m.%d %H:%M:%S")}"
      exif.imageuniqueid = @data[:phash]
      exif.save

      #Generate a date hash to be usen by the photofile model
      @datehash = generate_datehash(@data[:date_taken])

      #Create thumbnail and store it to the photofile
      if create_thumbnail()
        create_pf "tm"
      end

      #Create medium photo and store it to the photofile
      if resize_photo(@md_file.path, IMAGE_MEDIUM)
        create_pf "md"
      end

      #Create large photo and store it to the photofile
      if resize_photo(@lg_file.path, IMAGE_LARGE)
        create_pf "lg"
      end

      #Put the original photo in photofile
      create_pf "org"

      #Delete the source if import_mode is set
      if import_mode
        FileUtils.rm path
      end

    ensure
      #Close and unlink all tempfiles
      @org_file.close
      @org_file.unlink

      @tm_file.close
      @tm_file.unlink

      @lg_file.close
      @lg_file.unlink

      @md_file.close
      @md_file.unlink
    end

    return @data
  end

  def generate_datehash(date)
      datestring = date.strftime("%Y%m%d%H%M%S")
      unique = [*'a'..'z', *'A'..'Z', *0..9].shuffle.permutation(5).next.join

      datehash = {
        :datestring=>datestring,
        :unique=>unique,
        :year=>date.year,
        :month=>date.month,
        :day=>date.day
      }
      return datehash
  end

  def create_thumbnail()

    MiniMagick::Tool::Convert.new do |convert|
      convert.merge! [@org_file.path]
      convert.merge! ["-size", "200x200"]
      convert.merge! ["-thumbnail", "125x125^"]
      convert.merge! ["-gravity", "center"]
      convert.merge! ["-strip"]
      convert.merge! ["-interlace", "Plane"]
      convert.merge! ["-sampling-factor", "4:2:0"]
      convert.merge! ["-define", "jpeg:dct-method=float"]
      convert.merge! ["-quality", "85%"]
      convert.merge! ["+profile", "'*'"]
      convert.merge! ["-extent", "125x125"]
      convert << @tm_file.path
    end
    return true
  end

  def resize_photo(path, size)
    @image.resize size
    @image.write path
    return true
  end

  def create_pf(size)
    file=instance_variable_get("@#{size}_file")
    @datehash[:size] = size
    ps = @pf.create(file.path, @data[:date_taken])
    if not ps
      raise "Photofile for #{size} of #{@data[:date_taken]}"
    else
      @data["#{size}_id".to_sym] = ps[:id]
    end
  end

end
