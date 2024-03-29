# encoding: utf-8
class BatchQuery
  attr_reader :columns
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
    raise AttributeError.new("A connection adapter is obligatory!") if @conn.nil?

    @columns = @conn.columns(@table).collect{|c| c.name.to_sym}
    @idx = @columns.index(:id)
  end

  def query_string
    op = @last_id>0 ? ">" : ">="
    "select #{@fields_str} "        +
    "from #{@table} "               +
    "where (id #{op} #{@last_id}) " +
    "order by id asc "              +
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
    if @last_id > 0
      tmp = @last_id
      reset
      r = self.next
      @last_id = tmp
    else
      r = self.next
    end
    r
  end

  def reset
    @last_id = 0
  end

  def next
    result = @conn.exec_query(query_string)
    rows = result.rows
    @last_id = rows.last[@idx] if rows.last
    rows
  end

  def reverse_each
    raise NotImplementedError
  end
end
