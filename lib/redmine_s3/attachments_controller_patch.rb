module RedmineS3
  module AttachmentsControllerPatch
    def self.included(base) # :nodoc:
      base.extend(ClassMethods)
      base.send(:include, InstanceMethods)

      # Same as typing in the class
      base.class_eval do
        unloadable # Send unloadable so it will not be unloaded in development
        before_filter :download_from_s3, :except => [:destroy, :upload]
        skip_before_filter :file_readable
      end
    end

    module ClassMethods
    end

    module InstanceMethods
      def download_from_s3
        if @attachment.container.is_a?(Version) || @attachment.container.is_a?(Project)
          @attachment.increment_download
        end
        if RedmineS3::Connection.proxy?
          send_data RedmineS3::Connection.get(@attachment.disk_filename),
                                          :filename => filename_for_content_disposition(@attachment.filename),
                                          :type => detect_content_type(@attachment),
                                          :disposition => (@attachment.image? ? 'inline' : 'attachment')
        else
          redirect_to(RedmineS3::Connection.object_url(@attachment.disk_filename))
        end
      end
    end
  end
end
