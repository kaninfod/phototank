require 'resque/tasks'
require 'resque-loner'
require 'resque/scheduler/tasks'

task 'resque:setup' => :environment
