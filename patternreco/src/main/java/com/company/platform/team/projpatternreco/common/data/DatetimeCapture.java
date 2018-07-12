package com.company.platform.team.projpatternreco.common.data;

import java.time.LocalDateTime;
import java.util.*;

public class DatetimeCapture {
   Map<String, List<String>> captures;
   StringBuilder stringBuilder;
   int capturedStartPos;
   int capturedEndPos;

   private void initialize() {
       capturedStartPos = 0;
       capturedEndPos = 0;
       stringBuilder.setLength(0);
       captures = new HashMap<>();
   }

   public DatetimeCapture() {
      stringBuilder = new StringBuilder();
      initialize();
   }

   public boolean isNewCapture(DateTimeMatchedSlice matchedSlice) {
       if (matchedSlice.sliceStartPos != capturedEndPos) {
           return true;
       } else {
           return false;
       }
   }

   public void add(DateTimeMatchedSlice matchedSlice) {
       if (capturedEndPos == 0) {
           capturedStartPos = matchedSlice.sliceStartPos;
       }
     stringBuilder.append(matchedSlice.matchedGroup.getValue());
     List<String> valueList = captures.containsKey(matchedSlice.matchedGroup.getKey()) ?
             captures.get(matchedSlice.matchedGroup.getKey()) : new ArrayList<>();
     valueList.add(matchedSlice.matchedGroup.getValue());
     captures.put(matchedSlice.matchedGroup.getKey(), valueList);
     capturedEndPos = matchedSlice.sliceEndPos;
   }

   public void clear() {
       initialize();
   }



   public LocalDateTime tryParse() {
       return null;
   }
}
