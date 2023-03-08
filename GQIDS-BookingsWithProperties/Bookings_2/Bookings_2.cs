using System;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.ResourceManager.Objects;
using SLDataGateway.API.Querying;

[GQIMetaData(Name = "Bookings filtered on property value")]
public class SwitchesSource : IGQIDataSource, IGQIOnInit, IGQIInputArguments
{
    private readonly GQIDateTimeArgument _startArg = new GQIDateTimeArgument("Start") { IsRequired = true };
    private readonly GQIDateTimeArgument _endArg = new GQIDateTimeArgument("End") { IsRequired = true };

	// Filter bookings where property key equals property value
    private readonly GQIStringArgument _keyArg = new GQIStringArgument("Property key") { IsRequired = false };
    private readonly GQIStringArgument _valueArg = new GQIStringArgument("Property value") { IsRequired = false };

    private DateTime _start;
    private DateTime _end;
    private string _key;
    private string _value;
    private PagingCookie _pagingCookie = PagingCookie.Empty;
    private GQIDMS _dms;

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
            new GQIDateTimeColumn("Start"),
            new GQIDateTimeColumn("End"),
            new GQIStringColumn("ID"),
            new GQIStringColumn("Name"),
            new GQIStringColumn("Status"),
            new GQIStringColumn("Properties")
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        var filter = ConstructFilter();

        ClientRequestMessage request;
        if (_pagingCookie == PagingCookie.Empty)
        {
            request = new ManagerStoreStartPagingRequest<ReservationInstance>(filter.ToQuery(), 50);
        }
        else
        {
            request = new ManagerStoreNextPagingRequest<ReservationInstance>(filter.ToQuery(), _pagingCookie, 50);
        }

        var response = _dms.SendMessage(request) as ManagerStorePagingResponse<ReservationInstance>;
        _pagingCookie = response.PagingCookie;

        var rows = response.Objects.Select(o => ParseResult(o));
        return new GQIPage(rows.ToArray()) { HasNextPage = response.IsFinalPage };
    }

    private FilterElement<ReservationInstance> ConstructFilter()
    {
        var startFilter = ReservationInstanceExposers.Start.LessThan(_end);
        var endFilter = ReservationInstanceExposers.End.GreaterThan(_start);

        if (_key is null)
        {
            return new ANDFilterElement<ReservationInstance>(startFilter, endFilter);
        }

        var propertyFilter = ReservationInstanceExposers.Properties.DictStringField(_key).Equal(_value);
        return new ANDFilterElement<ReservationInstance>(propertyFilter, startFilter, endFilter);
    }

    private GQIRow ParseResult(ReservationInstance instance)
    {
        var start = new GQICell { Value = instance.Start };
        var end = new GQICell { Value = instance.End };
        var id = new GQICell { Value = instance.ID.ToString() };
        var name = new GQICell { Value = instance.Name };
        var status = new GQICell { Value = instance.Status.ToString() };

		// Visualize properties in a string column
        var strProps = instance.Properties.Select(kvp => $"{kvp.Key}: {kvp.Value}");
        var properties = new GQICell { Value = string.Join(",", strProps) };

        return new GQIRow(new GQICell[] { start, end, id, name, status, properties });
    }

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[] { _startArg, _endArg, _keyArg, _valueArg };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        _start = args.GetArgumentValue(_startArg);
        _end = args.GetArgumentValue(_endArg);

        _key = args.GetArgumentValue(_keyArg);
        _value = args.GetArgumentValue(_valueArg);

        return new OnArgumentsProcessedOutputArgs();
    }
}