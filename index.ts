import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginEvent, PluginMeta, Properties } from '@posthog/plugin-scaffold'
import { Pinpoint } from 'aws-sdk'
import { Event, PublicEndpoint, EventsBatch, PutEventsResponse } from 'aws-sdk/clients/pinpoint'
import { randomUUID } from 'crypto'

type PintpointPlugin = Plugin<{
    global: {
        pinpoint: Pinpoint
        buffer: ReturnType<typeof createBuffer>
        eventsToIgnore: Set<string>
    }
    config: {
        awsAccessKey: string
        awsSecretAccessKey: string
        awsRegion: string
        applicationId: string
        uploadSeconds: string
        uploadKilobytes: string
        eventsToIgnore: string
        maxAttempts: string
    }
    jobs: {}
}>

export const setupPlugin: PintpointPlugin['setupPlugin'] = (meta) => {
    const { global, config } = meta

    if (!config.awsAccessKey) {
        throw new Error('AWS access key missing!')
    }
    if (!config.awsSecretAccessKey) {
        throw new Error('AWS secret access key missing!')
    }
    if (!config.awsRegion) {
        throw new Error('AWS region missing!')
    }
    if (!config.applicationId) {
        throw new Error('ApplicationId missing!')
    }

    const uploadKilobytes = Math.max(1, Math.min(parseInt(config.uploadKilobytes) || 1, 100))
    const uploadSeconds = Math.max(1, Math.min(parseInt(config.uploadSeconds) || 1, 60))
    const maxAttempts = parseInt(config.maxAttempts)

    global.pinpoint = new Pinpoint({
        credentials: {
            accessKeyId: config.awsAccessKey,
            secretAccessKey: config.awsSecretAccessKey,
        },
        region: config.awsRegion,
    })

    global.buffer = createBuffer({
        limit: uploadKilobytes * 1024 * 1024,
        timeoutSeconds: uploadSeconds,
        onFlush: async (events) => {
            console.info(`Buffer flushed: ${JSON.stringify(events)}`)

            if (events?.length) {
                sendToPinpoint(events, meta)
            }
        },
    })

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )
}

export const teardownPlugin: PintpointPlugin['teardownPlugin'] = ({ global }) => {
    global.buffer.flush()
}

export const onEvent: PintpointPlugin['onEvent'] = (event, meta) => {
    let { global } = meta

    if (global.eventsToIgnore.has(event.event)) {
        return
    }

    global.buffer.add(event)
}

export const sendToPinpoint = async (events: PluginEvent[], meta: PluginMeta<PintpointPlugin>) => {
    const { config, global } = meta

    const command = {
        ApplicationId: config.applicationId,
        EventsRequest: {
            BatchItem: events.reduce((batchEvents: { [key: string]: EventsBatch }, e) => {
                let pinpointEvents = getEvents([e])
                let pinpointEndpoint = getEndpoint(e)
                let batchKey = pinpointEndpoint?.Address || randomUUID()

                if (batchEvents[batchKey]) {
                    pinpointEvents = { ...pinpointEvents, ...batchEvents[batchKey].Events }
                }

                batchEvents[batchKey] = {
                    Endpoint: pinpointEndpoint,
                    Events: pinpointEvents,
                }

                return batchEvents
            }, {}),
        },
    }

    console.log('Send Event')

    global.pinpoint.putEvents(command, (err: Error, data: PutEventsResponse) => {
        if (err) {
            console.error(`Error sending events to Pinpoint: ${err.message}:${JSON.stringify(command)}`)
        } else {
            console.info(
                `Uploaded ${events.length} event${events.length === 1 ? '' : 's'} to application ${
                    config.applicationId
                }`
            )
            console.info(`Response: ${JSON.stringify(data)}`)
        }
    })
}

export const getEndpoint = (event: PluginEvent): PublicEndpoint => {
    let endpoint = {}

    let channelType = 'EMAIL'
    let address = event.$set?.email
    if (event.event === 'APP - Device Link') {
        channelType = 'GCM'
        address = event.properties?.deviceToken
    }

    if (address) {
        endpoint = {
            ChannelType: channelType,
            Address: address,
            Attributes: {
                screen_density: [event.properties?.$screen_density?.toString() || ''],
                screen_height: [event.properties?.$screen_height?.toString() || ''],
                screen_name: [event.properties?.$screen_name?.toString() || ''],
                screen_width: [event.properties?.$screen_width?.toString() || ''],
                viewport_height: [event.properties?.$viewport_height?.toString() || ''],
                viewport_width: [event.properties?.$viewport_width?.toString() || ''],
            },
            Demographic: {
                AppVersion: event.properties?.$app_version,
                Locale: event.properties?.$locale,
                Make: event.properties?.$device_manufacturer || event.properties?.$device_type,
                Model: event.properties?.$device_model || event.properties?.$os,
                Platform: event.properties?.$os_name || event.properties?.$browser,
                PlatformVersion:
                    event.properties?.$os_version?.toString() || event.properties?.$browser_version?.toString(),
                Timezone: event.properties?.$geoip_time_zone,
            },
            EndpointStatus: 'ACTIVE',
            OptOut: 'NONE',
            EffectiveDate: event.timestamp || event.sent_at,
            Location: {
                City: event.properties?.$geoip_city_name,
                Country: event.properties?.$$geoip_country_code,
                Latitude: event.properties?.$geoip_latitude,
                Longitude: event.properties?.$geoip_longitude,
                PostalCode: event.properties?.$geoip_postal_code,
                Region: event.properties?.$geoip_subdivision_1_code,
            },
            Metrics: {},
            RequestId: event.uuid,
            User: {
                UserId: event.distinct_id,
                UserAttributes: Object.keys(event.$set ?? {}).reduce((attributes, key) => {
                    attributes = {
                        [key]: [getAttribute(event.$set, key)],
                        ...attributes,
                    }
                    return attributes
                }, {}),
            },
        }
    }

    return endpoint
}

export const getEvents = (events: PluginEvent[]): { [key: string]: Event } => {
    return events.reduce((pinpointEvents, event) => {
        console.info(`Event ${JSON.stringify(event)}`)

        const eventKey = event.uuid ?? randomUUID()
        const pinpointEvent = {
            AppTitle: event.properties?.$app_name || '',
            AppPackageName: event.properties?.$app_namespace || '',
            AppVersionCode: event.properties?.$app_version || '',

            // Number of attributes per event submitted should be less than 40.
            Attributes: Object.keys(event.properties ?? {})
                .filter((key) => !key.startsWith('$'))
                .reduce((attributes, key) => {
                    attributes = {
                        [key]: getAttribute(event.properties, key),
                        ...attributes,
                    }
                    return attributes
                }, {}),
            ClientSdkVersion: event.properties?.$lib_version,
            EventType: event.event.replace(/[#:?\/]/gi, '|'),
            // Metrics: {},
            SdkName: event.properties?.$lib,
            Timestamp: event.timestamp || event.sent_at,
            //Session: {},
            // Session: {
            //     Duration: 0,
            //     Id: 'string',
            //     StartTimestamp: 'string',
            //     StopTimestamp: ' string',
            // },
        }
        // if (event.properties?.$session_id) {
        //     pinpointEvent.Session = { Id: event.properties?.$session_id }
        // }
        pinpointEvents = {
            [eventKey]: pinpointEvent,
        }
        return pinpointEvents
    }, {})
}

const getAttribute = (properties: Properties | undefined, key: string): string => {
    let value = (properties ?? {})[key]
    if (typeof value === 'number' || typeof value === 'boolean') {
        value = value.toString()
    } else if (typeof value !== 'string') {
        value = JSON.stringify(value)
    }
    return value
}
