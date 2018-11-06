package zrz.triplerocks.jena;

import com.google.common.primitives.UnsignedBytes;

public enum JenaNodeType {

  IRI(0),
  BLANK_NODE(1),
  // for all other literal types, including those with custom dataType URIs.
  LITERAL(2);

  private byte code;

  private JenaNodeType(int code) {
    this.code = UnsignedBytes.checkedCast(code);
  }

  public byte code() {
    return this.code;
  }

  public static JenaNodeType valueOf(byte b) {
    return values()[b];

  }

}
