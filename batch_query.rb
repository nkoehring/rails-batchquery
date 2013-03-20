class BatchQuery
  include Enumerable

  def initialize(table, fields, batch_size=10, conn=nil)
    @table = table
    # Is fields a string? I want an array!
    if fields.respond_to? "join"
      @fields = fields
      @fields_str = fields.join(",")
    else
      @fields = fields.split(",")
      @fields_str = fields
    end
    @batch_size = batch_size
    @last_id = 0
    @conn = conn || ActiveRecord::Base.connection rescue nil
    raise ArgumentError.new("I need a connection handler!") if @conn.nil?
  end

  def query_string
    op = @last_id>0 ? ">" : ">="
    "select #{@fields_str} "       +
    "from #{@table} "             +
    "where (id #{op} #{@last_id}) " +
    "order by id asc "            +
    "limit #{@batch_size}"
  end

  def each
    loop do
      rows = self.next
      raise StopIteration if rows.length == 0
      yield rows
    end
  end
  
  def first
    tmp = @last_id
    reset
    r = self.next
    @last_id = tmp
    r
  end

  def reset
    @last_id = 0
  end

  def next
    result = @conn.exec_query(query_string)
    @columns ||= result.columns.collect(&:to_sym)
    rows = result.rows
    @last_id = rows.last[@columns.index(:id)] if rows.last
    rows
  end

  def reverse_each
    raise NotImplementedError
  end
end
