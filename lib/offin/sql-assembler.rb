require 'data_mapper'

# SqlAssembler is a helper class for PackageListPaginator.  It lets us
# gather up the parts of a basic SQL select statement in a re-usable
# way.  Presumably datamapper has been set up.

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

  def assemble

    # We assume exactly one select text; start out with this  'SELECT ...'

    sql_text = @select.text.first
    placeholder_values = @select.parameters

    # handle multiple conditions:  'WHERE ... AND ...'

    unless @where.text.length < 1
      sql_text += ' WHERE ' + @where.text[0]
    end

    unless @where.text.length < 2
      sql_text += ' AND ' + @where.text[1..-1].join(' AND ')
    end

    unless @where.parameters.empty?
      placeholder_values.push *@where.parameters
    end

    # we assume zero or one order and limit fragments

    # 'ORDER BY...'

    unless @order.text.empty?
      sql_text += ' ' + @order.text.first
    end

    unless @order.parameters.empty?
      placeholder_values.push *@order.parameters
    end

    # 'OFFSET ... LIMIT ...'

    unless @limit.text.empty?
      sql_text += ' ' + @limit.text.first
    end

    unless @limit.parameters.empty?
      placeholder_values.push *@limit.parameters
    end

    return sql_text, placeholder_values
  end
end
