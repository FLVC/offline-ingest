require 'data_mapper'

# SqlAssembler is a helper class for PackageListPaginator.  It lets us
# gather up the parts of a basic SQL select statement in a re-usable
# way.  Datamapper must have been set up to use this.

Struct.new('SqlFragment', :text, :parameters)

class SqlAssembler

  def initialize
    @select, @where, @order, @limit  = new_statement_fragment, new_statement_fragment, new_statement_fragment, new_statement_fragment
  end

  def add_condition text, *parameters
    add @where, text, *parameters
  end

  def set_select text, *parameters
    update @select, text, *parameters
  end

  def set_order text, *parameters
    update @order, text, *parameters
  end

  def set_limit  text, *parameters
    update @limit, text, *parameters
  end

  def execute
    sql, placeholder_values = assemble()
    return repository(:default).adapter.select(sql, *placeholder_values)
  end

  def dump
    sql, placeholder_values = assemble()
    return "SQL Assembler Dump: " + sql.inspect +  ",  " + placeholder_values.inspect
  end

  private

  def new_statement_fragment
    return Struct::SqlFragment.new([], [])
  end

  def update fragment, text, *parameters
    parameters ||= []
    fragment.text = [ text.strip ]
    fragment.parameters = parameters.flatten
  end

  def add fragment, text, *parameters
    parameters ||= []
    fragment.text.push text.strip
    fragment.parameters += parameters.flatten
  end


  # Take all the SQL fragments and assemble into

  def assemble

    # We'll only have one select text; start out with this  'SELECT ...'

    sql_text = @select.text.first
    placeholder_values = @select.parameters.clone

    # handle multiple conditions:  'WHERE ... AND ...'

    unless @where.text.length < 1
      sql_text += ' WHERE ' + @where.text[0]
    end

    unless @where.text.length < 2
      sql_text += ' AND ' + @where.text[1..-1].join(' AND ')
    end

    unless @where.parameters.empty?
      placeholder_values.push *@where.parameters.clone
    end

    # we assume zero or one order and limit fragments

    # 'ORDER BY...'

    unless @order.text.empty?
      sql_text += ' ' + @order.text.first
    end

    unless @order.parameters.empty?
      placeholder_values.push *@order.parameters.clone
    end

    # 'OFFSET ... LIMIT ...'

    unless @limit.text.empty?
      sql_text += ' ' + @limit.text.first
    end

    unless @limit.parameters.empty?
      placeholder_values.push *@limit.parameters.clone
    end

    return sql_text, placeholder_values
  end
end
