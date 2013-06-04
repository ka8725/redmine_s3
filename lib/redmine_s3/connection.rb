require 'aws-sdk'

AWS.config(:ssl_verify_peer => false)

module RedmineS3
  class Connection
    @@conn = nil
    @@s3_options = {
      :access_key_id     => nil,
      :secret_access_key => nil,
      :bucket            => nil,
      :endpoint          => nil,
      :private           => false,
      :expires           => nil,
      :secure            => false,
      :proxy             => false
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
        bucket = self.conn.buckets[self.bucket]
        bucket.create unless bucket.exists?
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

      def proxy?
        @@s3_options[:proxy]
      end

      def object(filename)
        bucket = self.conn.buckets[self.bucket]
        bucket.objects[filename]
      end

      def put(filename, data, content_type='application/octet-stream')
        object = self.object(filename)
        options = {}
        options[:acl] = :public_read unless self.private?
        options[:content_type] = content_type if content_type
        options[:content_disposition] = "inline; filename=#{ERB::Util.url_encode(filename)}"
        object.write(data, options)
      end

      def delete(filename)
        object = self.object(filename)
        object.delete if object.exists?
      end

      def object_url(filename)
        object = self.object(filename)
        if self.private?
          options = {:secure => self.secure?}
          options[:expires] = self.expires unless self.expires.nil?
          object.url_for(:read, options).to_s
        else
          object.public_url(:secure => self.secure?).to_s
        end
      end

      def get(filename)
        object = self.object(filename)
        object.read
      end
    end
  end
end
