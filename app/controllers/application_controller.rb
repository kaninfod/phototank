class ApplicationController < ActionController::API
  before_action :authenticate_request
   attr_reader :current_user

   private

   def authenticate_request
     if params.has_key? :token
       request.headers["Authorization"] = params[:token]
     end
     @current_user = AuthorizeApiRequest.call(request.headers).result
     render json: { error: 'Not Authorized' }, status: 401 unless @current_user
   end

end
