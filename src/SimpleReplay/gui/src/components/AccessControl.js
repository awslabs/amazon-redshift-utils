import React, {useEffect, useState} from 'react';
import {Box, FormField, RadioGroup, Select, SpaceBetween, StatusIndicator} from "@awsui/components-react";
import Input from "@awsui/components-react/input";
import Button from "@awsui/components-react/button";

export default function AccessControl({profiles}) {
    const [type, setType] = useState("profile");
    const [placeholder, setPlaceholder] = useState("");
    const [credentials, setCredentials] = useState("");
    const [disabled, setDisabled] = useState(true);
    const [saved, setSaved] = useState(false);
    const [selectedOption, setSelectedOption] = useState({label: 'default', value: 'default'});
    const options = profiles.map(item => ({label: item, value: item}))
    const [valid, setValid] = useState(true)


    useEffect(() => {
        function toggle() {
            if (type === "profile") {
                if (selectedOption.label !== "default") {
                    setDisabled(false);
                }
            } else if (type === "role") {
                setPlaceholder("arn:aws:iam::123456789012:role/customrole");
                setDisabled(false);
            }
        }

        toggle()
    }, [type, selectedOption]);

    function save() {
        if (type === "profile") {
            fetch(`/profile?name=${selectedOption.label}`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                        setValid(false)
                    } else {
                        setSaved(true)
                    }
                })

                .catch((error) => {
                    console.error('Error:', error);
                    setValid(false)

                });

        } else if (type === "role") {
            fetch(`/role?arn=${credentials}`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                        // TODO: Assume role Access denied
                        setValid(false)

                    } else {
                        setSaved(true)

                    }
                })
                .catch((error) => {
                    console.error('Error:', error);
                });
        }
    }


    return (
        <Box>
            <SpaceBetween size={"xs"}>
                <FormField label={"Credentials Type"}
                           description={"Provide an IAM user or role with access to S3. "}>
                    <RadioGroup
                        onChange={({detail}) => {
                            setSaved(false);
                            setType(detail.value);
                        }}
                        value={type}
                        items={[
                            {value: "profile", label: "Use a Profile"},
                            {value: "role", label: "Use an IAM Role"}
                        ]}/>
                </FormField>


                <FormField
                    label=""
                    errorText={!valid && "Unable to assume provided role. Please check credentials."}
                    secondaryControl={
                        <Button
                            variant={'primary'}
                            disabled={disabled}
                            onClick={() => save()}>
                            Save
                        </Button>}>


                    {type === "profile" &&

                        <Select
                            selectedOption={selectedOption}
                            onChange={({detail}) => {
                                setSaved(false);
                                setValid(true)
                                setSelectedOption(detail.selectedOption);
                            }
                            }
                            options={options}
                            selectedAriaLabel="Selected"
                            empty="No options"
                        />

                    }

                    {type === "role" &&

                        <Input value={credentials}
                               placeholder={placeholder}
                               type={'search'}

                               disabled={disabled}
                               onChange={(event) => {
                                   setSaved(false);
                                   setValid(true)
                                   setCredentials(event.detail.value)
                               }}></Input>
                    }

                    {saved &&

                        <StatusIndicator>Success</StatusIndicator>

                    }

                </FormField>
            </SpaceBetween>
        </Box>
    )
}