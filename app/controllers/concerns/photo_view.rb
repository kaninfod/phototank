module Authentication
  extend ActiveSupport::Concern

  included do

  end

  module ClassMethods
    def authenticate(email, password)
      user = find_by_email(email)
      user if user && user.authenticate(password)
    end
  end

  def create_password_reset_token
    logger.warn "Create password reset token code goes here."
    false
  end
end
