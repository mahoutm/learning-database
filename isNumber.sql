-- Function: is_number(text)
-- if you need more detail, use a exception logic to convert data type to numeric.

CREATE OR REPLACE FUNCTION is_number(num text)
  RETURNS boolean AS
$BODY$
BEGIN
RETURN textregexeq(num,E'^-*[[:digit:]]+(\\.[[:digit:]]+)?$');
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE;
