require 'dropbox_sdk'
class DropboxCatalog < Catalog
  before_destroy :delete_contents
  serialize :ext_store_data, Hash

  def auth
    self.appkey = Rails.configuration.x.dropbox["appkey"]
    self.appsecret = Rails.configuration.x.dropbox["appsecret"]
    base_url = self.redirect_uri
    url_ext = "/catalogs/authorize_callback"
    #params = "?type=FlickrCatalog&id=#{self.id}"
    self.redirect_uri = "#{base_url}#{url_ext}"
    self.save

    flow = DropboxOAuth2FlowNoRedirect.new(self.appkey, self.appsecret)
    authorize_url = flow.start()

  end

  def callback

    begin
      flow = DropboxOAuth2FlowNoRedirect.new(self.appkey, self.appsecret)
      access_token, user_id = flow.finish(self.verifier)
      self.access_token = access_token
      self.user_id = user_id
      self.save
      return 1
    rescue Exception => e
      return 0
    end
  end

  def import(use_resque=true)
    raise "Catalog is not online" unless online
    if not self.sync_from_catalog.blank?
        LocalCloneInstancesFromCatalogJob.perform_later self.id, self.sync_from_catalog
    end
  end

  def import_photo(photo_id)

    pf = PhotoFilesApi::Api::new

    photo = Photo.find(photo_id)
    instance = photo.instances.where(catalog_id: self.id).first
    photofile = pf.show(photo.org_id)
    dropbox_path = File.join(self.path, photofile[:path][0], photofile[:path][1], photofile[:path][2])
    dropbox_file = File.join(dropbox_path, "#{photofile[:path][3]}.jpg")

    if not instance.status #self.exists(dropbox_path)
      file = Tempfile.new("flickr.jpg")
      begin
        file.binmode
        file.write open(photofile[:url]).read
        src = file.path

        response = self.create_folder(dropbox_path)
        response = self.add_file(file.path, dropbox_file)

        instance.touch
        instance.update(
          catalog_id: self.id,
          path: response["path"],
          size: response["bytes"],
          rev: response["rev"],
          status: 1
        )
      end
    else
      raise "File exists in Dropbox with same revision id and path"
    end
  end

  def online
    true if access_token
  end

  def delete_contents
    #triggered when entire catalog is deleted
    instances.each do |instance|
      instance.destroy
    end
  end

  def delete_photo(photo_id)

    begin
      instance = self.instances.where(photo_id: photo_id).first
      if not instance.nil?
        self.client.file_delete instance.path unless instance.path.nil?
      end
      #instance.destroy
    rescue Exception => e

      logger.debug "#{e}"
    end
  end

  def appkey=(new_appkey)
    self.ext_store_data = self.ext_store_data.merge({:appkey => new_appkey})
  end

  def appkey
    self.ext_store_data[:appkey]
  end

  def appsecret=(new_appsecret)
    self.ext_store_data = self.ext_store_data.merge({:appsecret => new_appsecret})
  end

  def appsecret
    self.ext_store_data[:appsecret]
  end

  def redirect_uri=(new_redirect_uri)
    self.ext_store_data = self.ext_store_data.merge({:redirect_uri => new_redirect_uri})
  end

  def redirect_uri
    self.ext_store_data[:redirect_uri]
  end

  def verifier=(new_verifier)
    self.ext_store_data = self.ext_store_data.merge({:verifier => new_verifier})
  end

  def verifier
    self.ext_store_data[:verifier]
  end

  def access_token=(new_access_token)
    self.ext_store_data = self.ext_store_data.merge({:access_token => new_access_token})
  end

  def access_token
    self.ext_store_data[:access_token]
  end

  def user_id=(new_user_id)
    self.ext_store_data = self.ext_store_data.merge({:user_id => new_user_id})
  end

  def user_id
    self.ext_store_data[:user_id]
  end

  def client

    if not defined?(@client)
      @client = DropboxClient.new(self.access_token)
    end
    @client
  end

  def account_info
    self.client.account_info
  end

  def metadata(path)
    begin
      self.client.metadata(path)
    rescue
    end
  end

  def add_file(local_path, dropbox_path)
    self.client.put_file(dropbox_path, open(local_path), overwrite=true)
  end

  def add_file_in_chunks(dropbox_path, local_path)

    local_file_path = local_path
    dropbox_target_path = dropbox_path
    chunk_size = 4*1024*1024
    local_file_size = File.size(local_file_path)
    uploader = self.client.get_chunked_uploader(File.new(local_file_path, "r"), local_file_size)
    retries = 0
    puts "Uploading..."

    while uploader.offset < uploader.total_size
      begin
        uploader.upload(chunk_size)
      rescue DropboxError => e
        if retries > 10
          puts "- Error uploading, giving up."
          break
        end
        puts "- Error uploading, trying again..."
        retries += 1
      end
    end
    puts "Finishing upload..."
    uploader.finish(dropbox_target_path)

    puts "Done."
  end

  def create_folder(path)
    begin
      self.client.file_create_folder(path)
    rescue DropboxError => e
      self.metadata(path)
    end
  end

  def exists(path)
    response = self.metadata(path)
    if not response.nil?
      if Instance.where(rev: response["rev"]).present?
        return true
      else
        return false
      end
    end
    return false
  end

end
