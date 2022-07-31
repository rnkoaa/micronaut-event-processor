package io.richard.event

trait ContextFixture {

    Map<String, Object> getConfiguration() {
        Map<String, Object> configs = [:]
        if (specName) {
            configs["spec.name"] = specName
        }

        if (additionalProperties != null) {
            configs += additionalProperties
        }

        return configs
    }

     Map<String, Object> getAdditionalProperties(){
         return null
     }

    String getSpecName() {
        null
    }

}