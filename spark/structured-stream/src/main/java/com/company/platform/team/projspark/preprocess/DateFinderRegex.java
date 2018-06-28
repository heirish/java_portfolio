package com.company.platform.team.projspark.preprocess;

import com.company.platform.team.projspark.data.DateTimeMatchedSlice;
import com.company.platform.team.projspark.data.MatchedSlice;
import com.company.platform.team.projspark.utils.DateTimeUtil;
import javafx.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by admin on 2018/6/25.
 */
public class DateFinderRegex {
    private static final String DAYS = "monday|tuesday|wednesday|thursday|friday|saturday|sunday"
            + "|mon|tue|tues|wed|thur|thurs|fri|sat|sun";
    private static final String MONTHS = "january|february|march|april|may|june|july|august|september"
            + "|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec";
    private static final String TIMEZONES = "ACDT|ACST|ACT|ACWDT|ACWST|ADDT|ADMT|ADT|AEDT|AEST|AFT"
            + "|AHDT|AHST|AKDT|AKST|AKTST|AKTT|ALMST|ALMT|AMST|AMT|ANAST|ANAT|ANT|APT"
            + "|AQTST|AQTT|ARST|ART|ASHST|ASHT|AST|AWDT|AWST|AWT|AZOMT|AZOST|AZOT|AZST|AZT"
            + "|BAKST|BAKT|BDST|BDT|BEAT|BEAUT|BIOT|BMT|BNT|BORT|BOST|BOT|BRST|BRT|BST|BTT"
            + "|BURT|CANT|CAPT|CAST|CAT|CAWT|CCT|CDDT|CDT|CEDT|CEMT|CEST|CET|CGST|CGT"
            + "|CHADT|CHAST|CHDT|CHOST|CHOT|CIST|CKHST|CKT|CLST|CLT|CMT|COST|COT|CPT"
            + "|CST|CUT|CVST|CVT|CWT|CXT|ChST|DACT|DAVT|DDUT|DFT|DMT|DUSST|DUST|EASST|EAST|EAT"
            + "|ECT|EDDT|EDT|EEDT|EEST|EET|EGST|EGT|EHDT|EMT|EPT|EST|ET|EWT|FET|FFMT|FJST|FJT"
            + "|FKST|FKT|FMT|FNST|FNT|FORT|FRUST|FRUT|GALT|GAMT|GBGT|GEST|GET|GFT|GHST|GILT"
            + "|GIT|GMT|GST|GYT|HAA|HAC|HADT|HAE|HAP|HAR|HAST|HAT|HAY|HDT|HKST|HKT|HLV|HMT"
            + "|HNA|HNC|HNE|HNP|HNR|HNT|HNY|HOVST|HOVT|HST|ICT|IDDT|IDT|IHST|IMT|IOT"
            + "|IRDT|IRKST|IRKT|IRST|ISST|IST|JAVT|JCST|JDT|JMT|JST|JWST|KART|KDT|KGST"
            + "|KGT|KIZST|KIZT|KMT|KOST|KRAST|KRAT|KST|KUYST|KUYT|KWAT|LHDT|LHST|LINT"
            + "|LKT|LMT|LMT|LMT|LMT|LRT|LST|MADMT|MADST|MADT|MAGST|MAGT|MALST|MALT|MART"
            + "|MAWT|MDDT|MDST|MDT|MEST|MET|MHT|MIST|MIT|MMT|MOST|MOT|MPT|MSD|MSK|MSM"
            + "|MST|MUST|MUT|MVT|MWT|MYT|NCST|NCT|NDDT|NDT|NEGT|NEST|NET|NFT|NMT|NOVST"
            + "|NOVT|NPT|NRT|NST|NT|NUT|NWT|NZDT|NZMT|NZST|OMSST|OMST|ORAST|ORAT|PDDT"
            + "|PDT|PEST|PET|PETST|PETT|PGT|PHOT|PHST|PHT|PKST|PKT|PLMT|PMDT|PMMT|PMST"
            + "|PMT|PNT|PONT|PPMT|PPT|PST|PT|PWT|PYST|PYT|QMT|QYZST|QYZT|RET|RMT|ROTT"
            + "|SAKST|SAKT|SAMT|SAST|SBT|SCT|SDMT|SDT|SET|SGT|SHEST|SHET|SJMT|SLT|SMT"
            + "|SRET|SRT|SST|STAT|SVEST|SVET|SWAT|SYOT|TAHT|TASST|TAST|TBIST|TBIT|TBMT"
            + "|TFT|THA|TJT|TKT|TLT|TMT|TOST|TOT|TRST|TRT|TSAT|TVT|ULAST|ULAT|URAST"
            + "|URAT|UTC|UYHST|UYST|UYT|UZST|UZT|VET|VLAST|VLAT|VOLST|VOLT|VOST|VUST"
            + "|VUT|WARST|WART|WAST|WAT|WDT|WEDT|WEMT|WEST|WET|WFT|WGST|WGT|WIB|WIT"
            + "|WITA|WMT|WSDT|WSST|WST|WT|XJT|YAKST|YAKT|YAPT|YDDT|YDT|YEKST|YEKST"
            + "|YEKT|YEKT|YERST|YERT|YPT|YST|YWT|zzz";
    // explicit north american timezones that get replaced
    private static final String NA_TIMEZONES = "pacific|eastern|mountain|central";
    private static final String NUM_TIMEZONES = "(\\s*\\+\\d{1,4})";
    //TODO:也可以将NUM_TIMEZONES放到解析完以后
    private static final String ALL_TIMEZONES = TIMEZONES + "|" + NA_TIMEZONES + "|" + NUM_TIMEZONES;
    //private static final String DELIMITERS = "[/\\:\\-\\,\\s\\_\\+\\@]+";
    private static final String DELIMITERS = "[/:\\-,\\s_+@]+";
    private static final String TIME_PERIODS = "a\\.m\\.|am|p\\.m\\.|pm";

    //private static final String TIME_REGEX = "("
    //        + "("  // format XX:YY(:ZZ) (PM) (EST)
    //        // + "(?<hours>\\d{1,2})\\:(?<minutes>\\d{1,2})(\\:(?<seconds>\\d{1,2}))?([\\.\\,](?<microseconds>\\d{1,6})?"
    //        + "(?<hours>\\d{1,2}):(?<g1_minutes>\\d{1,2})(:(?<seconds>\\d{1,2}))?([.,](?<microseconds>\\d{1,6}))?"
    //        + "\\s*(?<timeperiods>" + TIME_PERIODS + ")?"
    //        + "\\s*(?<timezones>" + ALL_TIMEZONES + ")?"
    //        + ")"
    //        + "|" // format 11 AM (EST)
    //        + "((?<hours>\\d{1,2})\\s*((?<timeperiods>" + TIME_PERIODS + ")|(?<timezones>" + ALL_TIMEZONES + ")))"
    //        + ")";
    private static final String TIME_REGEX = "("
            + "("  // format XX:YY(:ZZ) (PM) (EST)
            + "(\\d{1,2}:\\d{1,2}(:\\d{1,2})?([\\.,]\\d{1,6})?)"
            + "(\\s*" + TIME_PERIODS + ")?"
            + "(\\s*" + ALL_TIMEZONES + ")?"
            + ")"
            + "|" // format 11 AM (EST)
            // + "(\\d{1,2}(\\s*" + TIME_PERIODS + ")?(\\s*" + ALL_TIMEZONES + ")?)"
            + "(\\d{1,2}((\\s*" + TIME_PERIODS + ")|(\\s*" + ALL_TIMEZONES + ")))"
            + ")";
    //private static final String DATETIME_REGEX = "("
    //        + "(?<time>" + TIME_REGEX + ")" //should at the first place
    //        + "|(?<digits>\\d+)"
    //        + "|(?<days>" + DAYS + ")"
    //        + "|(?<months>" + MONTHS + ")"
    //        + "|(?<delimiters>" + DELIMITERS + ")"
    //        + "|(?<extratokens>z|t)"
    //        + "){3,}";
    private static final Map<String, String> regexMap = new LinkedHashMap<>();
    private static Pattern DATETIME_PATTERN;

    public DateFinderRegex() {
        regexMap.put("time", TIME_REGEX);
        regexMap.put("digits", "\\d+");
        regexMap.put("days", DAYS);
        regexMap.put("months", MONTHS);
        regexMap.put("delimiters", DELIMITERS);
        regexMap.put("extratokens", "z|t");
        StringBuilder stringBuilder = new StringBuilder();;
        stringBuilder.append("(");
        for(Map.Entry<String, String> entry : regexMap.entrySet()) {
            stringBuilder.append(String.format("(?<%s>%s)|", entry.getKey(), entry.getValue()));
        }
        if (stringBuilder.length() > 0) {
            stringBuilder.setLength(stringBuilder.length()-1);
        }
        stringBuilder.append("){3,}");
        System.out.println(stringBuilder.toString());
        DATETIME_PATTERN = Pattern.compile(stringBuilder.toString(),
                Pattern.CASE_INSENSITIVE|Pattern.MULTILINE|Pattern.DOTALL|Pattern.UNICODE_CASE);
    }

    public List<MatchedSlice> findDateTimes(String text) {
        List<MatchedSlice> slices = new ArrayList<>();
        Matcher matcher = DATETIME_PATTERN.matcher(text);
        int lastEndIndex = 0;
        int startIndex = 0;
        StringBuilder datetimeStringBuilder = new StringBuilder();
        while (matcher.find()) {
            // another chunk begin, check last chunk
            if (lastEndIndex > 0 && matcher.start() != lastEndIndex) {
                 if (datetimeStringBuilder.length() > 0
                     && isValidDatetime(datetimeStringBuilder.toString())) {
                    MatchedSlice matchedSlice = new MatchedSlice();
                    matchedSlice.matchedString = datetimeStringBuilder.toString();
                    matchedSlice.startIndex = startIndex;
                    matchedSlice.endIndex = lastEndIndex;
                }
                datetimeStringBuilder.setLength(0);
                startIndex = matcher.start();
            }
            lastEndIndex = matcher.end();
            datetimeStringBuilder.append(matcher.group());
            System.out.println(String.format(" detail matched string: [%s], beg: %s, end: %s",
                    matcher.group(), matcher.start(), matcher.end()));
        }

        if (datetimeStringBuilder.length() > 0
                && isValidDatetime(datetimeStringBuilder.toString())) {
            MatchedSlice matchedSlice = new MatchedSlice();
            matchedSlice.matchedString = datetimeStringBuilder.toString();
            matchedSlice.startIndex = startIndex;
            matchedSlice.endIndex = lastEndIndex;
        }

        return slices;
    }

    private void printCaptures(Map<String, List<String>>captures) {
        for (Map.Entry<String, List<String>> entry : captures.entrySet()) {
            System.out.println(String.format("%s : %s", entry.getKey(), entry.getValue().toString()));
        }
    }

    private DateTimeMatchedSlice getMathedSlice(Matcher matcher) {
        DateTimeMatchedSlice matchedSlice = new DateTimeMatchedSlice();
        for (Map.Entry<String, String> entry : regexMap.entrySet()) {
            String groupName = entry.getKey();
            try {
                matchedSlice.matchedGroup = new Pair<>(groupName, matcher.group(groupName));
                matchedSlice.sliceStartPos = matchedSlice.sliceStartPos;
                matchedSlice.sliceEndPos = matchedSlice.sliceEndPos;
                return matchedSlice;
            } catch (IllegalArgumentException e) {

            } catch (IllegalStateException e) {

            }
        }
        return null;
    }

    private boolean isValidDatetime(String datetimeString) {
        if (StringUtils.isEmpty(datetimeString)
                || datetimeString.length() < 3) {
            return false;
        }

        try {
            DateTimeUtil.tryParse(datetimeString);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
