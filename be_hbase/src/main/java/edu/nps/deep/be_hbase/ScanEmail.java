// Scan for email addresses, put them in Features.

package edu.nps.deep.be_hbase;

import java.lang.StringBuilder;
import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;

/**
 * Reads all email features in one split and puts them in Features.
 */
public final class ScanEmail {

  private final long splitOffset;
  private final int splitSize;
  private final String filename;
  private static final HashSet<String> domainNames = getDomainNames();

  private char[] buffer;

  public Features features;

  private static HashSet<String> getDomainNames() {
    String names = "AC|AD|AE|AERO|AF|AG|AI|AL|AM|AN|AO|AQ|AR|ARPA|AS|ASIA|AT|AU|AW|AX|AZ|BA|BB|BD|BE|BF|BG|BH|BI|BIZ|BJ|BL|BM|BN|BO|BR|BS|BT|BV|BW|BY|BZ|CA|CAT|CC|CD|CF|CG|CH|CI|CK|CL|CM|CN|CO|COM|COOP|CR|CU|CV|CX|CY|CZ|DE|DJ|DK|DM|DO|DZ|EC|EDU|EE|EG|EH|ER|ES|ET|EU|FI|FJ|FK|FM|FO|FR|GA|GB|GD|GE|GF|GG|GH|GI|GL|GM|GN|GOV|GP|GQ|GR|GS|GT|GU|GW|GY|HK|HM|HN|HR|HT|HU|ID|IE|IL|IM|IN|INFO|INT|IO|IQ|IR|IS|IT|JE|JM|JO|JOBS|JP|KE|KG|KH|KI|KM|KN|KP|KR|KW|KY|KZ|LA|LB|LC|LI|LK|LR|LS|LT|LU|LV|LY|MA|MC|MD|ME|MF|MG|MH|MIL|MK|ML|MM|MN|MO|MOBI|MP|MQ|MR|MS|MT|MU|MUSEUM|MV|MW|MX|MY|MZ|NA|NAME|NC|NE|NET|NF|NG|NI|NL|NO|NP|NR|NU|NZ|OM|ORG|PA|PE|PF|PG|PH|PK|PL|PM|PN|PR|PRO|PS|PT|PW|PY|QA|RE|RO|RS|RU|RW|SA|SB|SC|SD|SE|SG|SH|SI|SJ|SK|SL|SM|SN|SO|SR|ST|SU|SV|SY|SZ|TC|TD|TEL|TF|TG|TH|TJ|TK|TL|TM|TN|TO|TP|TR|TRAVEL|TT|TV|TW|TZ|UA|UG|UK|UM|US|UY|UZ|VA|VC|VE|VG|VI|VN|VU|WF|WS|YE|YT|YU|ZA|ZM|ZW";
    String[] nameArray = names.split("\\|");
    HashSet<String> nameSet = new HashSet<String>();
    for (String name : nameArray) {
      nameSet.add(name);
    }
    return nameSet;
  }


  public ScanEmail(SplitReader splitReader) throws IOException {
    // values from splitReader
    splitOffset = splitReader.getSplitOffset();
    splitSize = (int)splitReader.getSplitSize();
    filename = splitReader.getFilename();

    // the split as char array
    // https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
    buffer = new char[splitSize];

    // storage for the features
    features = new Features();

    splitReader.read(buffer, 0, splitSize);

    for (int i=1; i< splitSize - 1; i++) {
      if (buffer[i] == '@') {
        if (buffer[i+1] != '\0') {
          // unicode 8
          int start = findStart(i);
          if (start == i) {
            continue;
          }
          int stop = findStop(i);
          if (stop == i) {
            continue;
          }

          // build email address from this
          String feature = new String(buffer, start, stop-start+1);

          // store the feature
          maybePutFeature(feature, start);

        } else {
          // unicode 16
          int start = findStart16(i);
          if (start == i) {
            continue;
          }
          int stop = findStop16(i);
          if (stop == i) {
            continue;
          }

          // build unicode 8 email address from this
          StringBuilder sb = new StringBuilder();
          for (int j=start; j <= stop+1; j+=2) {
            sb.append(buffer[j]);
          }

          // store the feature
          maybePutFeature(sb.toString(), start);

        }
      }
    }
  }

  private void maybePutFeature(String feature, int start) {

    // find last dot, it will preceed the domain name
    final int dotIndex = feature.lastIndexOf('.');
    if (dotIndex == -1) {
      // not a valid feature
      return;
    }

    // require to recognize the domain name
    if (!domainNames.contains(feature.substring(dotIndex+1).toUpperCase(Locale.ENGLISH))) {
      // no valid domain name
      return;
    }

    // here, so accept the feature
    features.add(new Feature(filename,
                             Long.toString(start+splitOffset), feature));
  }

  // from https://en.wikipedia.org/wiki/Email_address
  // Use RFC 5322 except do not recognize backslash or quoted string.
  // Specifically: local part <= 64 characters, domain <= 255 characters and
  // not space or "(),:;<>@[\]

  // find local part of email address, return start else "at" point
  private int findStart(int at) {
    int start = at;
    while (true) {
      // done if at beginning
      if (start == 0) {
        return start;
      }

      // invalid if local part > 64 bytes long
      if (at - start > 64) {
        return at;
      }

      // done if next char is invalid
      char c = buffer[start-1];
      if (c<0x20 || c >0x7f || c=='\"' || c=='(' || c==')' || c==',' ||
          c==':' || c==';' || c=='<' || c=='>' || c=='@' || c=='[' ||
          c=='\\' || c==']') {
        return start;
      }

      // char is valid so step backwards to it
      --start;
    }
  }
           
  // find domain part of email address, return stop else "at" point
  private int findStop(int at) {
    int stop = at;
    while (true) {
      // done if at EOF
      if (stop == splitSize - 1) {
        return stop;
      }

      // invalid if domain part > 256 bytes long
      if (stop - at > 256) {
        return at;
      }

      // done if next char is invalid
      char c = buffer[stop+1];
      if (c<0x20 || c >0x7f || c=='\"' || c=='(' || c==')' || c==',' ||
          c==':' || c==';' || c=='<' || c=='>' || c=='@' || c=='[' ||
          c=='\\' || c==']') {
        return stop;
      }

      // char is valid so step forward to it
      ++stop;
    }
  }
           
  // find local part of email address, return start else "at" point
  private int findStart16(int at) {
    int start = at;
    while (true) {
      // done if at beginning
      if (start <= 1) {
        return start;
      }

      // invalid if local part > 64 bytes long
      if (at - start > 64 * 2) {
        return at;
      }

      // done if next char pair is invalid
      if (buffer[start-1] != '\0') {
        return start;
      }
      char c = buffer[start-2];
      if (c<0x20 || c >0x7f || c=='\"' || c=='(' || c==')' || c==',' ||
          c==':' || c==';' || c=='<' || c=='>' || c=='@' || c=='[' ||
          c=='\\' || c==']') {
        return start;
      }

      // char pair is valid so step backwards to it
      start -= 2;
    }
  }
           
  // find domain part of email address, return stop else "at" point
  // where stop is first byte of last pair
  private int findStop16(int at) {
    int stop = at;
    while (true) {
      // done if at EOF
      if (stop >= splitSize - 2) {
        return stop;
      }

      // invalid if domain part > 256 bytes long
      if (stop - at > 256 * 2) {
        return at;
      }

      // done if next char is invalid
      if (buffer[stop+2] != '\0') {
        return stop;
      }
      char c = buffer[stop+1];
      if (c<0x20 || c >0x7f || c=='\"' || c=='(' || c==')' || c==',' ||
          c==':' || c==';' || c=='<' || c=='>' || c=='@' || c=='[' ||
          c=='\\' || c==']') {
        return stop;
      }

      // char is valid so step forward to it
      stop += 2;
    }
  }
}

