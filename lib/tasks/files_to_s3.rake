namespace :redmine_s3 do
  task :files_to_s3 => :environment do
    require 'thread'

    # updates a single file on s3
    def update_file_on_s3(attachment, objects)
      if File.exists?(attachment.diskfile)
        object = objects[attachment.disk_filename]

        # get the file modified time, which will stay nil if the file doesn't exist yet
        # we could check if the file exists, but this saves a head request
        s3_digest = object.etag[1...-1] rescue nil 

        # put it on s3 if the file has been updated or it doesn't exist on s3 yet
        if s3_digest.nil? || s3_digest != attachment.digest
          puts "Put file " + attachment.disk_filename
          File.open(attachment.diskfile, 'rb') do |fileObj|
            RedmineS3::Connection.put(attachment, fileObj)
          end
          # If you really know what you are doing
          # TODO : Maybe add a task option for this ?
          # File.delete(attachment.diskfile)
        else
          puts attachment.disk_filename + ' is up-to-date on S3'
        end
      else
        puts attachment.disk_filename + ' is already migrated on S3'
      end
    end

    # enqueue all of the files to be "worked" on
    attachments = Attachment.find(:all)

    # init the connection, and grab the ObjectCollection object for the bucket
    conn = RedmineS3::Connection.establish_connection
    objects = conn.buckets[RedmineS3::Connection.bucket].objects

    # create some threads to start syncing all of the queued files with s3
    threads = Array.new
    8.times do
      threads << Thread.new do
        while !attachments.empty?
          update_file_on_s3(attachments.pop, objects)
        end
      end
    end
    
    # wait on all of the threads to finish
    threads.each do |thread|
      thread.join
    end

  end
end
