# table_catalog            | postgres_to_redshift
# table_schema             | public
# table_name               | films
# column_name              | description
# ordinal_position         | 2
# column_default           |
# is_nullable              | YES
# data_type                | character varying
# character_maximum_length | 255
# character_octet_length   | 1020
# numeric_precision        |
# numeric_precision_radix  |
# numeric_scale            |
# datetime_precision       |
# interval_type            |
# interval_precision       |
# character_set_catalog    |
# character_set_schema     |
# character_set_name       |
# collation_catalog        |
# collation_schema         |
# collation_name           |
# domain_catalog           |
# domain_schema            |
# domain_name              |
# udt_catalog              | postgres_to_redshift
# udt_schema               | pg_catalog
# udt_name                 | varchar
# scope_catalog            |
# scope_schema             |
# scope_name               |
# maximum_cardinality      |
# dtd_identifier           | 2
# is_self_referencing      | NO
# is_identity              | NO
# identity_generation      |
# identity_start           |
# identity_increment       |
# identity_maximum         |
# identity_minimum         |
# identity_cycle           |
# is_generated             | NEVER
# generation_expression    |
# is_updatable             | YES
#
class PostgresToRedshift::Column
  attr_accessor :attributes

  ARRAY_TYPE = 'array'.freeze

  CAST_TYPES_FOR_COPY = {
    "text" => "CHARACTER VARYING(65535)",
    "json" => "CHARACTER VARYING(65535)",
    "bytea" => "CHARACTER VARYING(65535)",
    "money" => "DECIMAL(19,2)",
    "oid" => "CHARACTER VARYING(65535)",
    "inet" => "CHARACTER VARYING(65535)",
    "array" => "CHARACTER VARYING(65535)",
    "uuid" => "CHARACTER VARYING(65535)",
    "jsonb" => "CHARACTER VARYING(65535)",
  }

  def initialize(attributes: )
    self.attributes = attributes
  end

  def name
    attributes["column_name"]
  end

  def name_for_copy
    if data_type.downcase == ARRAY_TYPE
      %Q[CAST(array_to_json("#{name}") AS #{data_type_for_copy}) AS #{name}]
    elsif needs_type_cast?
      %Q[CAST("#{name}" AS #{data_type_for_copy}) AS #{name}]
    else
      %Q["#{name}"]
    end
  end

  def data_type
    attributes["data_type"]
  end

  def data_type_for_copy
    data_type_adjusted = data_type

    numeric_precision = attributes["numeric_precision"]
    numeric_scale = attributes["numeric_scale"]
    if (data_type == 'numeric' && numeric_scale && numeric_precision)
      data_type_adjusted = "NUMERIC(#{numeric_precision},#{numeric_scale})"
    end


    CAST_TYPES_FOR_COPY[data_type_adjusted.downcase] || data_type_adjusted
  end

  private
  def needs_type_cast?
    data_type != data_type_for_copy
  end
end
