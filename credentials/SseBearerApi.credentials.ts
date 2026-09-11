import type { Icon, ICredentialType, INodeProperties } from 'n8n-workflow';

export class SseBearerApi implements ICredentialType {
	name = 'sseBearerApi';

	displayName = 'SSE Bearer Auth API';

	documentationUrl =
		'https://github.com/suneetk92/n8n-nodes-resilient-sse?tab=readme-ov-file#credentials';

	icon: Icon = {
		light: 'file:../nodes/ResilientSseTrigger/resilient_sse.svg',
		dark: 'file:../nodes/ResilientSseTrigger/resilient_sse.dark.svg',
	};

	properties: INodeProperties[] = [
		{
			displayName: 'Bearer Token',
			name: 'token',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'Sent to the SSE endpoint as an "Authorization: Bearer <token>" header',
		},
	];
}
