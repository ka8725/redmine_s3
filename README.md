# S3 plugin for Redmine

## Description
This [Redmine](http://www.redmine.org) plugin makes file attachments be stored on [Amazon S3](http://aws.amazon.com/s3) rather than on the local filesystem. This is a fork for [original gem](http://github.com/tigrish/redmine_s3) and difference is that this one supports Redmine 2

## Installation
1. Make sure Redmine is installed and cd into it's root directory
2. `git clone git://github.com/ka8725/redmine_s3.git plugins/redmine_s3`
3. `cp plugins/redmine_s3/config/s3.yml.example config/s3.yml`
4. Edit config/s3.yml with your favourite editor
5. Restart mongrel/upload to production/whatever
6. *Optional*: Run `rake redmine_s3:files_to_s3` to upload files in your files folder to s3
7. `rm -Rf plugins/redmine_s3/.git` 

## Options Overview
* The bucket specified in s3.yml will be created automatically when the plugin is loaded (this is generally when the server starts).
* *Deprecated* (no longer supported, specify endpoint option instead) If you have created a CNAME entry for your bucket set the cname_bucket option to true in s3.yml and your files will be served from that domain.
* After files are uploaded they are made public, unless private is set to true.
* Public and private files can use HTTPS urls using the secure option
* Files can use private signed urls using the private option
* Private file urls can expire a set time after the links were generated using the expires option
* If you're using a Amazon S3 clone, then you can do the download relay by using the proxy option.

## Options Detail
* access_key_id: string key (required)
* secret_access_key: string key (required)
* bucket: string bucket name (required)
* endpoint: string endpoint instead of s3.amazonaws.com
* secure: boolean true/false
* private: boolean true/false
* expires: integer number of seconds for private links to expire after being generated
* proxy: boolean true/false
* Defaults to private: false, secure: false, proxy: false, default endpoint, and default expires
