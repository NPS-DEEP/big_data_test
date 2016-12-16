// Ported from bulk_extractor/src/be13_api/unicode_escape.cpp

package edu.nps.deep.be_cluster;

public class UnicodeEscape {
  private static String hexesc(final char ch) {
    return String.format("0x%02x", (int)ch);
  }

  /** returns true if this is a UTF8 continuation character */
  private static boolean utf8cont(final char ch) {
    return ((ch&0x80)==0x80) &  ((ch & 0x40)==0);
  }

  /**
   * After a UTF-8 sequence is decided, this function is called
   * to determine if the character is invalid. The UTF-8 spec now
   * says that if a UTF-8 decoding produces an invalid character, or
   * a surrogate, it is not valid. (There were some nasty security
   * vulnerabilities that were exploited before this came out.)
   * So we do a lot of checks here.
   */
  private static boolean valid_utf8codepoint(long unichar) {
    // Check for invalid characters in the bmp
    if (unichar == 0xfffe) return false;         // reversed BOM
    if (unichar == 0xffff) return false;

    if(unichar >= 0xd800 && unichar <=0xdfff) return false; // high and low surrogates
    if(unichar < 0x10000) return true;  // looks like it is in the BMP

    // check some regions outside the bmp

    // Plane 1:
    if(unichar > 0x13fff && unichar < 0x16000) return false;
    if(unichar > 0x16fff && unichar < 0x1b000) return false;
    if(unichar > 0x1bfff && unichar < 0x1d000) return false;
        
    // Plane 2
    if(unichar > 0x2bfff && unichar < 0x2f000) return false;
    
    // Planes 3--13 are unassigned
    if(unichar >= 0x30000 && unichar < 0xdffff) return false;

    // Above Plane 16 is invalid
    if(unichar > 0x10FFFF) return false;        // above plane 16?
    
    return true;                        // must be valid
  }

/**
 * validateOrEscapeUTF8
 * Input: UTF8 string (possibly corrupt)
 * Input: do_escape, indicating whether invalid encodings shall be escaped.
 * Note:
 *    - if not escaping but an invalid encoding is present and DEBUG_PEDANTIC is set, then assert() is called.
 *    - DO NOT USE wchar_t because it is 16-bits on Windows and 32-bits on Unix.
 * Output: 
 *   - UTF8 string.  If do_escape is set, then corruptions are escaped in \xFF notation where FF is a hex character.
 */

  public static String validateOrEscapeUTF8(final String input,
                                            final boolean escape_bad_utf8,
                                            final boolean escape_backslash) {

    // skip the validation if not escaping and not DEBUG_PEDANTIC
    if (escape_bad_utf8==false && escape_backslash==false) {
      return input;
    }
        
    // validate or escape input
    StringBuilder output = new StringBuilder();
    for (int i=0; i<input.length(); ) {
      char ch = input.charAt(i);
        
      // utf8 1 byte prefix (0xxx xxxx)
      if((ch & 0x80)==0x00){          // 00 .. 0x7f
        if(ch=='\\' && escape_backslash){   // escape the escape character as \x92
          output.append(hexesc(ch));
          i++;
          continue;
        }

        if( ch < ' '){              // not printable are escaped
          output.append(hexesc(ch));
          i++;
          continue;
        }
        output.append(ch);          // printable is not escaped
        i++;
        continue;
      }

      // utf8 2 bytes  (110x xxxx) prefix
      if(((ch & 0xe0)==0xc0)  // 2-byte prefix
           && (i+1 < input.length())
           && utf8cont((uint8_t)input.charAt(i+1))){
        uint32_t unichar = ((input.charAt(i) & 0x1f) << 6) | ((input.charAt(i+1) & 0x3f));

        // check for valid 2-byte encoding
        if(valid_utf8codepoint(unichar)
               && (input.charAt(i)!=0xc0)
               && (unichar >= 0x80)){ 
          output.append(input.charAt(i++));        // byte1
          output.append(input.charAt(i++));        // byte2
          continue;
        }
      }
                
      // utf8 3 bytes (1110 xxxx prefix)
      if(((ch & 0xf0) == 0xe0)
           && (i+2 < input.length())
           && utf8cont(input.charAt(i+1))
           && utf8cont(input.charAt(i+2))){
        uint32_t unichar = ((input.charAt(i) & 0x0f) << 12)
                | ((input.charAt(i+1) & 0x3f) << 6)
                | ((input.charAt(i+2) & 0x3f));
            
        // check for a valid 3-byte code point
        if(valid_utf8codepoint(unichar)
               && unichar>=0x800){                     
          output.append(input.charAt(i++));        // byte1
          output.append(input.charAt(i++));        // byte2
          output.append(input.charAt(i++));        // byte3
          continue;
        }
      }
            
      // utf8 4 bytes (1111 0xxx prefix)
      if((( ch & 0xf8) == 0xf0)
           && (i+3 < input.length())
           && utf8cont(input.charAt(i+1))
           && utf8cont(input.charAt(i+2))
           && utf8cont(input.charAt(i+3))){
        uint32_t unichar =( ((input.charAt(i) & 0x07) << 18)
                                |((input.charAt(i+1) & 0x3f) << 12)
                                |((input.charAt(i+2) & 0x3f) <<  6)
                                |((input.charAt(i+3) & 0x3f)));

        if(valid_utf8codepoint(unichar) && unichar>=0x1000000){
          output.append(input.charAt(i++));        // byte1
          output.append(input.charAt(i++));        // byte2
          output.append(input.charAt(i++));        // byte3
          output.append(input.charAt(i++));        // byte4
          continue;
        }
      }

      if (escape_bad_utf8) {
        // Just escape the next byte and carry on
        output.append(input.charAt(i++));
      } else {
        // drop the next byte
      }
    }
    return output.toString();
  }
}

