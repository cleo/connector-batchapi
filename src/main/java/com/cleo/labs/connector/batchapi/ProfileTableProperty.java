package com.cleo.labs.connector.batchapi;

import static com.cleo.connector.array.ArrayFeature.OrderingColumns;

import com.cleo.connector.api.annotations.Array;
import com.cleo.connector.api.annotations.Display;
import com.cleo.connector.api.annotations.Property;
import com.cleo.connector.api.interfaces.IConnectorProperty;
import com.cleo.connector.api.property.PropertyBuilder;
import com.google.common.base.Strings;
import com.google.gson.Gson;

/**
 * Profile table extended property - @Array of subproperties (identified by @Property)
 */
@Array(features = { OrderingColumns }) 
public class ProfileTableProperty {

    private static final Gson GSON = new Gson();

    /**
     * Display value for the Profile Table property
     * @param value the Routing Table property value (a JSON array)
     * @return "n Records" (or "1 Record")
     */
    @Display
    public String display(String value) {
        int size = toProfiles(value).length;
        return String.format("%d Profile%s", size, size==1?"":"s");
    }
  
    @Property
    final IConnectorProperty<Boolean> enabled = new PropertyBuilder<>("Enabled", true)
        .setRequired(true)
        .build();

    @Property
    final IConnectorProperty<String> profileName = new PropertyBuilder<>("ProfileName", "")
        .setDescription("The profile name.")
        .build();

    @Property
    final public IConnectorProperty<String> url = new PropertyBuilder<>("Url", "")
        .setRequired(true)
        .setDescription("The Harmony server URL including protocol, "+
                        "host and port, e.g. \"https://harmony.example.com:6080\".")
        .build();

    @Property
    final IConnectorProperty<String> user = new PropertyBuilder<>("User", "")
        .setRequired(true)
        .setDescription("The Harmony API user.")
        .build();

    @Property
    final IConnectorProperty<String> password = new PropertyBuilder<>("Password", "")
        .setRequired(true)
        .setDescription("The Harmony API password.")
        .addAttribute(IConnectorProperty.Attribute.Password)
        .build();

    @Property
    final IConnectorProperty<Boolean> ignoreTLSChecks = new PropertyBuilder<>("IgnoreTLSChecks", false)
        .setDescription("Select to ignore TLS checks on trusted certificates and hostname matching.")
        .build();

    /**
     * Deserialize the JSON array into a Java {@code Profile[]}.
     * @param value the JSON array (may be {@code null})
     * @return a {@code Profile[]}, may be {@code Profile[0]}, but never {@code null}
     */
    public static Profile[] toProfiles(String value) {
        return Strings.isNullOrEmpty(value) ? new Profile[0] : GSON.fromJson(value, Profile[].class);
    }
}