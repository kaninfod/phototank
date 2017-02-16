class Job < ActiveRecord::Base
  serialize :arguments, Array

  scope :distinct_job_types, -> {
    ary = select(:job_type).distinct.map { |c| [c.job_type] }.unshift([''])
    ary.delete([nil])
    ary.sort_by{|el| el[0] }
  }
end
