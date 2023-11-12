/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.desmind.processors.timezonelookup;

import com.sun.source.tree.UsesTree;
import net.iakovlev.timeshape.TimeZoneEngine;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class TimeZoneLookup extends AbstractProcessor {


    public static final PropertyDescriptor PROP_CONVERT_TIMESTAMPS = new PropertyDescriptor
            .Builder().name("Timestamps to Convert")
            .description("names of attributes that contain nifi-std formatted timestamps. list is comma seperated.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_ATTR_LATITUDE = new PropertyDescriptor
            .Builder().name("latitude attribute")
            .description("latitude attribute")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor PROP_ATTR_LONGITUDE = new PropertyDescriptor
            .Builder().name("longitude attribute")
            .description("longitude attribute")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("successfully found a timezone")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("failed to lookup timezone")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private TimeZoneEngine timeZoneEngine;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(PROP_CONVERT_TIMESTAMPS);
        descriptors.add(PROP_ATTR_LATITUDE);
        descriptors.add(PROP_ATTR_LONGITUDE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);

        timeZoneEngine = TimeZoneEngine.initialize();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();

        String latitudeAttr = context.getProperty(PROP_ATTR_LATITUDE).getValue();
        String longitudeAttr = context.getProperty(PROP_ATTR_LONGITUDE).getValue();
        String[] tsAttributesToConvert = context.getProperty(PROP_CONVERT_TIMESTAMPS).getValue().split(",");


        double latitude;
        double longitude;
        try {
            latitude = Double.parseDouble(flowFile.getAttribute(latitudeAttr));
            longitude = Double.parseDouble(flowFile.getAttribute(longitudeAttr));
        } catch (Exception e){
            session.putAttribute(flowFile, "Fail", "cannot parse latitude or longitude from attribute");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        List<ZoneId> zoneIdList = timeZoneEngine.queryAll(latitude, longitude);

        if(!zoneIdList.isEmpty()) {
            for (String tsAttribute :
                    tsAttributesToConvert) {

                LocalDateTime localDate;

                try {
                    localDate = LocalDateTime.parse(context.getProperty(tsAttribute).getValue(), DateTimeFormatter.ISO_INSTANT);
                } catch (Exception e) {
                    session.putAttribute(flowFile, "Fail", "property " + tsAttribute + " cannot be parsed");
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }

                int minTsOffset = 432001;
                int maxTsOffset = -432001;

                for(ZoneId zoneId: zoneIdList){
                    int offset = zoneId.getRules().getOffset(localDate).getTotalSeconds();
                    if(offset < minTsOffset){
                        minTsOffset = offset;
                    }
                    if(offset > maxTsOffset){
                        maxTsOffset = offset;
                    }
                }

                int tzOffsetMiddle = Math.round((float) (minTsOffset + maxTsOffset) /2);
                int tzDelta = maxTsOffset - tzOffsetMiddle;

                session.putAttribute(flowFile, tsAttribute+".localized.median", localDate.plusSeconds(tzOffsetMiddle).format(DateTimeFormatter.ISO_INSTANT));
                session.putAttribute(flowFile, tsAttribute+".localized.delta", String.format("%d Seconds", tzDelta));
            }
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
