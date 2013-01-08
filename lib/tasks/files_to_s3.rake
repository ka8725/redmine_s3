namespace :redmine_s3 do
  task :files_to_s3 => :environment do
    require 'thread'

    # updates a single file on s3
    def update_file_on_s3(attachment, objects)
      if File.exists?(attachment.diskfile)
        object = objects[attachment.disk_filename]

        # get the file ETag, which will stay nil if the file doesn't exist yet
        # we could check if the file exists, but this saves a head request
        s3_digest = object.etag[1...-1] rescue nil 

        # put it on s3 if the file has been updated or it doesn't exist on s3 yet
        if s3_digest.nil?
          puts "Sending file #{attachment.disk_filename}..."
        elsif s3_digest != attachment.digest 
          puts "Updating file #{attachment.disk_filename}..."
        else
          # puts attachment.disk_filename + ' is up-to-date on S3'
          return
        end

        File.open(attachment.diskfile, 'rb') do |data|
          RedmineS3::Connection.put(attachment, data)
        end
      else
        # puts attachment.disk_filename + ' is already migrated on S3'
      end
    end

    # Get all of the attachments to be "worked" on
    # TODO : batch process in order to avoid loading
    # everything in RAM
    attachments = Attachment.find(:all)

    # Initialize progress info
    start_time = Time.now
    todo = attachments.length
    done = 0
    errors = 0
    last_done_pct = 0
    mutex = Mutex.new

    # init the connection, and grab the ObjectCollection object for the bucket
    conn = RedmineS3::Connection.establish_connection
    objects = conn.buckets[RedmineS3::Connection.bucket].objects

    # create some threads to start syncing all of the queued attachments with s3
    threads = Array.new
    8.times do
      threads << Thread.new do
        while true do
          # Get attachment, bail out
          # if no more work
          attachment = mutex.synchronize do
              attachments.pop
          end
          break if attachment.nil?
          
          # Actual work
          error = 0
          begin
            update_file_on_s3(attachment, objects)
          rescue => e
            puts "Problem with #{attachment.disk_filename} : #{e}"
            puts e.backtrace.join "\n"
            error = 1
          end

          # Report progress
          mutex.synchronize do
            done = done + 1
            errors = errors + error

            done_pct = (100.0 * done / todo).to_i
            if done_pct > last_done_pct
              eta = Time.now + 1.0 * (todo - done) * (Time.now - start_time) / done
              puts ">>> #{done} files (#{done_pct}%) done, #{errors} errors... ETA #{eta}"
              last_done_pct = done_pct
            end
          end
        end
      end
    end
    
    # wait on all of the threads to finish
    threads.each do |thread|
      thread.join
    end
  end
end
