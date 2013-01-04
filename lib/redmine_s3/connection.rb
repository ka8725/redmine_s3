require 'aws-sdk'
require 'stringio'

AWS.config(:ssl_verify_peer => false)

module RedmineS3
  class TransferError < RuntimeError
  end

  class Connection
    @@conn = nil
    @@bucket = nil
    @@s3_options = {
      :access_key_id     => nil,
      :secret_access_key => nil,
      :bucket            => nil,
      :endpoint          => nil,
      :private           => false,
      :expires           => nil,
      :secure            => false
    }

    class << self
      def load_options
        file = ERB.new( File.read(File.join(Rails.root, 'config', 's3.yml')) ).result
        YAML::load( file )[Rails.env].each do |key, value|
         @@s3_options[key.to_sym] = value
        end
      end

      def establish_connection
        load_options unless @@s3_options[:access_key_id] && @@s3_options[:secret_access_key]
        options = {
          :access_key_id => @@s3_options[:access_key_id],
          :secret_access_key => @@s3_options[:secret_access_key]
        }
        options[:s3_endpoint] = self.endpoint unless self.endpoint.nil?
        @conn = AWS::S3.new(options)
      end

      def conn
        @@conn || establish_connection
      end

      def bucket
        load_options unless @@s3_options[:bucket]
        @@s3_options[:bucket]
      end

      def create_bucket
        if !@@bucket
          @@bucket = self.conn.buckets[self.bucket]
          if !@@bucket.exists?
            @@bucket = self.conn.buckets.create(self.bucket)
          end
        end
        @@bucket
      end

      def endpoint
        @@s3_options[:endpoint]
      end

      def expires
        @@s3_options[:expires]
      end

      def private?
        @@s3_options[:private]
      end

      def secure?
        @@s3_options[:secure]
      end

      def put(attachment, data)
        # if data is a string, turns it into a buffer
        if !data.respond_to?(:read)
          data = StringIO.new(data)
        end

        do_put = Proc.new do
          # Allows retries, if supported
          if data.respond_to?(:seek)
            data.seek(0)
          end

          # Object options
          options = {
            :content_disposition => "inline; filename='#{ERB::Util.url_encode(attachment.filename)}'",
            :content_type => attachment.content_type,
            :content_length => data.size,
            # Prevent multipart upload, so that the object ETag is the content's MD5
            :single_request => true
          }
          options[:acl] = :public_read unless self.private?

          # Streaming upload, we'll compute the MD5 while we're at it
          md5 = Digest::MD5.new
          object = self.create_bucket.objects[attachment.disk_filename]
          object = object.write(options) do |buffer, bytes|
            if read = data.read(bytes)
              md5.update(read)
              buffer.write(read)
            end
          end

          # Check digests
          md5 = md5.hexdigest
          if !(attachment.digest.nil? || attachment.digest == md5)
            puts "WARNING : wrong digest for file #{attachment.disk_filename}"
            # TODO : update attachment.digest ?
          end

          s3_md5 = self.try("get MD5 for #{object.key}") { object.etag[1...-1] }
          if s3_md5 != md5
            raise TransferError.new("MD5 mismatch for file #{attachment.disk_filename}, local=#{md5}, S3=#{s3_md5}")
          end

          # Returns the MD5
          md5
        end

        # Try multiple times, if possible (i.e. data is seekable)
        if data.respond_to?(:seek)
          self.try("sending #attachment.disk_filename}", &do_put)
        else
          do_put.call
        end
      end

      def try(name, max_tries=3, base_wait=0.25, exponent=2, &block)
        try = 0
        wait = base_wait
        while true
          begin
            try = try + 1
            return block.call
          rescue => e
            raise if try==max_tries
            puts "WARNING : #{name} failed due to #{e} (try #{try})"
            sleep(wait)
            wait = wait * exponent
          end
        end
      end

      def delete(filename)
        object = self.conn.buckets[self.bucket].objects[filename]
        object.delete if object.exists?
      end

      def object_url(filename)
        object = self.conn.buckets[self.bucket].objects[filename]
        if self.private?
          options = {:secure => self.secure?}
          options[:expires] = self.expires unless self.expires.nil?
          object.url_for(:read, options).to_s
        else
          object.public_url(:secure => self.secure?).to_s
        end
      end
    end
  end
end
