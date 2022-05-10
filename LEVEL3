from pprint import pformat
import metrosdk
from metrosdk import Mapper as M
from metrosdk import Distributor as D


HOST_URI = "metro-web:3000"
SECRET_KEY = "AWS_SECRET_KEY"
ACCESS_ID = "AWS_ACCESS_ID"


def sdk_call(m, *args, **kwargs):
    print("calling {}({},{}".format(m.__name__, args, kwargs))
    rsp = m(*args, **kwargs)
    print("rsp:\n{}".format(pformat(rsp)))
    return rsp


def main():
    client = metrosdk.MetroClient(host_uri=HOST_URI)
    bp_id = sdk_call(client.create_blueprint, name="LVL3", desc="LVL3")["id"]
    with metrosdk.PipelineSpecBuilder(pipeline_name="LVL3") as b:
        src1 = metrosdk.S3Src(
            comp_id="s1",
            bucket="metrolink-bucket-1",
            secret_key=SECRET_KEY,
            access_id=ACCESS_ID,
            region="us-east-1",
            prefix="Micromobility/InputBikes",
            data_format=metrosdk.DataFormat.JSON_LINE,
        )

        src2 = metrosdk.S3Src(
            comp_id="s2",
            bucket="metrolink-bucket-1",
            secret_key=SECRET_KEY,
            access_id=ACCESS_ID,
            region="us-east-1",
            prefix="Micromobility/InputRides",
            data_format=metrosdk.DataFormat.JSON_LINE,
        )

        dst1 = metrosdk.S3Dst(
            comp_id="t1",
            bucket="metrolink-bucket-1",
            secret_key=SECRET_KEY,
            access_id=ACCESS_ID,
            region="us-east-1",
            key_name="Micromobility/OutputShop1/OutModel",
            data_format=metrosdk.DataFormat.CSV,
            batch_size=100,
        )

        dst2 = metrosdk.S3Dst(
            comp_id="t2",
            bucket="metrolink-bucket-1",
            secret_key=SECRET_KEY,
            access_id=ACCESS_ID,
            region="us-east-1",
            key_name="Micromobility/OutputShop2/OutAll",
            data_format=metrosdk.DataFormat.CSV,
            batch_size=100,
        )

        mapper1 = M(comp_id="m1")
        mapper1.rule = M.ObjectRule(
            {
                "BikeId": M.RefRule("/data/BikeId"),
                "BikeIncomeMin": 80,
                "BirthYear": M.RefRule("/data/BirthYear"),
                "EndStationId": M.RefRule("/data/EndStationId"),
                "EndStationLatitude": M.RefRule("/data/EndStationLatitude"),
                "EndStationLongitude": M.RefRule("/data/EndStationLongitude"),
                "EndStationName": M.RefRule("/data/EndStationName"),
                "Gender": M.RefRule("/data/Gender"),
                "StartStationId": M.RefRule("/data/StartStationId"),
                "StartStationLatitude": M.RefRule("/data/StartStationLatitude"),
                "StartStationLongitude": M.RefRule("/data/StartStationLongitude"),
                "StartStationName": M.RefRule("/data/StartStationName"),
                "StartTime": M.RefRule("/data/StartTime"),
                "StopTime": M.RefRule("/data/StopTime"),
                "TripDuration": M.RefRule("/data/TripDuration"),
                "UserType": M.RefRule("/data/UserType"),
            }
        )

        mapper2 = M(comp_id="m2")
        mapper2.rule = M.ObjectRule(
            {
                "BikeId": M.RefRule("/data/BikeId"),
                "BikeIncome": M.RefRule("/data/BikeIncome"),
                "BikeModel": M.RefRule("/data/BikeModel"),
                "EnterShop": M.RefRule("/data/EnterShop"),
                "FirstRide": M.RefRule("/data/FirstRide"),
                "LeaveShop": M.RefRule("/data/LeaveShop"),
                "PurchaseDate": M.RefRule("/data/PurchaseDate"),
            }
        )

        agg = metrosdk.Aggregator(comp_id="agg", alias="AggBikes")
        agg.add_index_rule(alias="BikesBID", path="/data/BikeId", field_type="number")
        agg.add_index_rule(
            alias="BikeIncome", path="/data/BikeIncome", field_type="number"
        )
        agg.set_purge(ttl_seconds=0, max_records=1000, max_memory_mb=1000)

        corr = metrosdk.Correlator(
            comp_id="corr", purge_on_hit=False, is_opposite_match=False
        )
        rule = corr.create_rule(
            min_results=1,
            name="CorrelationRule1",
            description='Correlates between "BikeId" coming from'
            + '"CorrBikeId" and the "BikesBID" coming from "AggBikes" aggregator',
        )
        rule.add_stream_number_expression(
            agg_id=agg.id,
            agg_key="/data/BikeId",
            is_mandatory=True,
            stream_path="/data/BikeId",
            op="=",
        )
        rule.add_stream_number_expression(
            agg_id=agg.id,
            agg_key="/data/BikeIncome",
            is_mandatory=True,
            stream_path="/data/BikeIncomeMin",
            op=">",
        )

        mapper3 = M(comp_id="m3")
        mapper3.rule = M.ObjectRule(
            {
                "BikeId": M.RefRule("/metadata/AggBikes[0]/BikeId"),
                "BikeIncome": M.RefRule("/metadata/AggBikes[0]/BikeIncome"),
                "BikeModel": M.RefRule("/metadata/AggBikes[0]/BikeModel"),
                "BirthYear": M.RefRule("/data/BirthYear"),
                "EndStationId": M.RefRule("/data/EndStationId"),
                "EndStationLatitude": M.RefRule("/data/EndStationLatitude"),
                "EndStationLongitude": M.RefRule("/data/EndStationLongitude"),
                "EndStationName": M.RefRule("/data/EndStationName"),
                "EnterShop": M.RefRule("/metadata/AggBikes[0]/EnterShop"),
                "FirstRide": M.RefRule("/metadata/AggBikes[0]/FirstRide"),
                "Gender": M.RefRule("/data/Gender"),
                "LeaveShop": M.RefRule("/metadata/AggBikes[0]/LeaveShop"),
                "PurchaseDate": M.RefRule("/metadata/AggBikes[0]/PurchaseDate"),
                "StartStationId": {"__ref": ".data.StartStationId"},
                "StartStationLatitude": M.RefRule("/data/StartStationLatitude"),
                "StartStationLongitude": M.RefRule("/data/StartStationLongitude"),
                "StartStationName": M.RefRule("/data/StartStationName"),
                "StartTime": M.RefRule("/data/StartTime"),
                "StopTime": M.RefRule("/data/StopTime"),
                "TripDuration": M.RefRule("/data/TripDuration"),
                "UserType": M.RefRule("/data/UserType"),
            }
        )

        dist = D(comp_id="d")
        dist.rule = [
            D.OutputRule(
                name="BMX_374t",
                first_match=False,
                cond=D.OpRule(
                    "=", D.RefRule("/data/BikeModel"), D.ConstRule("BMX_374t")
                ),
            ),
            D.OutputRule(
                name="All",
                first_match=True,
                cond=D.OpRule("=", D.ConstRule("true"), D.ConstRule("true")),
            ),
        ]

        src1 >> mapper1 >> corr >> mapper3 >> dist
        src2 >> mapper2 >> agg
        dist >> [dst1, dst2]

    rsp = sdk_call(client.create_pipeline, bp_id=bp_id, pipeline_spec=b.spec)
    pipeline_id = rsp["id"]
    sdk_call(
        client.wait_for_pipeline_status,
        bp_id,
        pipeline_id,
        metrosdk.PipelineStatus.RUNNING,
    )


if __name__ == "__main__":
    main()
