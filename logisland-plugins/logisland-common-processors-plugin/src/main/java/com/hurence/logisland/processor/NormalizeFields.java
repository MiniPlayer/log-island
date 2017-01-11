package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"record", "fields", "normalizer"})
@CapabilityDescription("Changes the name of a field according to a provided name mapping\n" +
        "...")
@DynamicProperty(name = "alternative mapping",
        supportsExpressionLanguage = true,
        value = "a comma separated list of possible field name",
        description = "when a field has a name contained in the list it will be renamed with this property field name")
public class NormalizeFields extends AbstractProcessor {


    private static final Logger logger = LoggerFactory.getLogger(NormalizeFields.class);


    public static final AllowableValue DO_NOTHING =
            new AllowableValue("do_nothing", "nothing to do", "leave record as it was");

    public static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    public static final AllowableValue KEEP_ONLY_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field and delete the other", "keep only old field and delete the other");

    public static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("waht to do when a field with the same name already exists ?")
            .required(true)
            .defaultValue(DO_NOTHING.getValue())
            .allowableValues(DO_NOTHING, OVERWRITE_EXISTING, KEEP_ONLY_OLD_FIELD)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(CONFLICT_RESOLUTION_POLICY);
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        Map<String, String[]> fieldsNameMapping = getFieldsNameMapping(context);
        for (Record record : records) {
            normalizeRecord(context, record, fieldsNameMapping);
        }
        return records;
    }


    private void normalizeRecord(ProcessContext context, Record record, Map<String, String[]> fieldsNameMapping) {

        String conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();


        fieldsNameMapping.keySet().forEach(normalizedFieldName -> {

            final String[] obsoleteFieldNames = fieldsNameMapping.get(normalizedFieldName);

            // field is already here
            if (record.hasField(normalizedFieldName)) {
                if (conflictPolicy.equals(KEEP_ONLY_OLD_FIELD.getValue())) {
                    for (String obsoleteFieldName : obsoleteFieldNames) {
                        if (record.hasField(obsoleteFieldName)) {
                            record.removeField(obsoleteFieldName);
                        }
                    }
                } else if (conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                    for (String obsoleteFieldName : obsoleteFieldNames) {
                        overwriteObsoleteFieldName(record, normalizedFieldName, obsoleteFieldName);
                    }
                }
            } else {
                // loop over obsolete field names
                for (String obsoleteFieldName : obsoleteFieldNames) {
                    overwriteObsoleteFieldName(record, normalizedFieldName, obsoleteFieldName);
                }
            }
        });
    }

    private void overwriteObsoleteFieldName(Record record, String normalizedFieldName, String obsoleteFieldName) {
        // remove old badly named field
        if (record.hasField(obsoleteFieldName)) {
            final Field fieldToRename = record.getField(obsoleteFieldName);
            record.removeField(obsoleteFieldName);
            record.setField(normalizedFieldName, fieldToRename.getType(), fieldToRename.getRawValue());
        }
    }

    private Map<String, String[]> getFieldsNameMapping(ProcessContext context) {
        /**
         * list alternative regex
         */
        Map<String, String[]> fieldsNameMappings = new HashMap<>();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String fieldName = entry.getKey().getName();
            final String[] mapping = entry.getValue().split(",");

            fieldsNameMappings.put(fieldName, mapping);
        }
        return fieldsNameMappings;
    }


}
