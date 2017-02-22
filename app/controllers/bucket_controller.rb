class BucketController < ApplicationController
  include BucketActions

  def add
    if Photo.where(id: params[:id]).present?
      Bucket.create(user: @current_user.id, photo_id: params[:id])
      @bucket = Bucket.where(user: @current_user.id)
      render :status => 200, json: {
        msg: "Photo added to bucket",
        photo_id: params[:id],
        bucket: @bucket
      }
    else
      render :status => 404, json: {error: "photo not found"}
    end

  end

  def remove
    entry = Bucket.where(user: @current_user.id, photo_id: params[:id])
    if entry.count == 1
      entry.first.destroy
      @bucket = Bucket.where(user: @current_user.id)
      render :status => 200, json: {
        msg: "Photo removed from bucket",
        photo_id: params[:id],
        bucket: @bucket
      }
    else
      render :status => 404, json: {error: "photo not in bucket"}
    end
  end

  def toggle
    entry = Bucket.where(user: @current_user.id, photo_id: params[:id])
    if entry.count == 1
      entry.first.destroy
      code = 200
      msg = "Photo removed from bucket"
      action= 'removed'
    elsif Photo.where(id: params[:id]).present?
      Bucket.create(user: @current_user.id, photo_id: params[:id])
      code = 201
      msg = "Photo added to bucket"
      action= 'added'
    else
      code = 404
      msg = "can't do anything"
      action= 'error'
    end
    @bucket = Bucket.where(user: @current_user.id)
    render :status => code, json: {action: action, msg: msg, photo_id: params[:id], bucket: @bucket}

  end

  def clear
    session[:bucket] = []
    redirect_to bucket_path
  end

  def count
    render :json => {'count' => session[:bucket].count}
  end

  def index
    @bucket = Bucket.where(user: @current_user.id)
    # @photos = Photo.where(id:@bucket).page params[:page]
    #If this was requested from an ajax call it should be rendered with slim view
    # if request.xhr?
    #   render :partial=>"photos/grid"
    # end
  end

  def list
    @bucket = get_bucket
    @photos_in_bucket = Photo.where(id:@bucket)
    @photos_in_bucket =  @photos_in_bucket.index_by(&:id).values_at(*@bucket)
    respond_to do |format|
      format.html {
        render :partial => "list"
      }
      format.json {
        render json: {:bucket=>@bucket}
      }
    end

  end

  def save_to_album
    if params.has_key? :album_id
      if params[:album_id].to_i == -1
        album = Album.new
        album.name = "Saved from bucket"
        album.photo_ids = session[:bucket]
        album.save
      else
        album = Album.find params[:album_id]
        album.photo_ids = [*album.photo_ids, *session[:bucket]]
        album.save
      end
    end
    render :json => {:status => "OK"}

  end

  def delete_photos
    session[:bucket].each do |photo_id|
      Photo.find(photo_id).delete
    end
    @bucket = get_bucket
    session[:bucket] = []
    respond_to do |format|
      format.html {
        redirect_to bucket_path
      }
      format.json {
        render json: {:bucket=>@bucket}
      }
    end
  end

  def rotate

    @bucket = get_bucket
    rotate_helper(@bucket, params[:degrees])
    respond_to do |format|
      format.html {
        redirect_to bucket_path
      }
      format.json {
        render json: {:bucket=>@bucket}
      }
    end
  end

  def edit
    session[:finalurl] = request.referer
    @submit_path = "/bucket/update"
    @photo = Photo.new
  end

  def update
    @bucket = get_bucket
    Photo.find(@bucket).each do |photo|
      #update the database entry
      if photo.update(params.permit(:date_taken, :location_id))
        #update the exif data on the original photo
        PhotoUpdateExif.perform_later photo.id
      end
    end
    redirect_to session[:finalurl]
  end

  def like
    session[:bucket].each do |photo_id|
      photo = Photo.find(photo_id)
      photo.liked_by current_user
    end
    render :json => {:status => "OK"}
  end

  def unlike
    session[:bucket].each do |photo_id|
      photo = Photo.find(photo_id)
      photo.unliked_by current_user
    end
    render :json => {:status => "OK"}
  end

  def add_comment
    if params.has_key? "comment"
      session[:bucket].each do |photo_id|
        comment = add_comment_helper(photo_id, params[:comment])
      end
    end
    render :json => {:status => "OK"}
  end


  private


  def get_bucket
    if session.include? 'bucket'
      session[:bucket]
    else
      session[:bucket] = []
      session[:bucket]
    end
  end


end
